/*
Copyright 2026 The Tekton Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package taskrun

import (
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/tektoncd/pipeline/pkg/apis/config"
	v1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"github.com/tektoncd/pipeline/test"
	"github.com/tektoncd/pipeline/test/diff"
	"github.com/tektoncd/pipeline/test/parse"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/system"
)

const (
	spotRetryPodName  = "test-spot-retry-pod"
	stepFailed255     = `"step-simple-step" exited with code 255`
	stepFailed1       = `"step-simple-step" exited with code 1`
	nodeShutdownCause = "pod lost to node shutdown on node-a"
)

const gkeRemovedNodeCause = "pod deleted by GKE after node node-a was removed"

var nodeShutdownCondition = corev1.PodCondition{
	Type:    corev1.DisruptionTarget,
	Status:  corev1.ConditionTrue,
	Reason:  corev1.PodReasonTerminationByKubelet,
	Message: nodeShutdownMessage,
}

func fixtureAnnotations() map[string]string {
	return map[string]string{fixtureAnnotation: "abb-studio-1"}
}

// spotRetryTaskRun returns a running TaskRun whose attempt runs in spotRetryPodName.
func spotRetryTaskRun(t *testing.T) *v1.TaskRun {
	t.Helper()
	return parse.MustParseV1TaskRun(t, `
metadata:
  name: test-spot-retry
  namespace: foo
spec:
  taskRef:
    name: test-task
status:
  startTime: "2021-12-31T23:59:00Z"
  podName: test-spot-retry-pod
  conditions:
  - type: Succeeded
    status: Unknown
    reason: Running
    message: Not all Steps in the Task have finished executing
  taskSpec:
    steps:
    - name: simple-step
      image: foo
      command: ["/mycmd"]
`)
}

// failedSpotPod returns the attempt's pod with its step finished finishedAgo before the test clock.
func failedSpotPod(exitCode int32, finishedAgo time.Duration, conditions ...corev1.PodCondition) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: spotRetryPodName, Namespace: "foo"},
		Spec:       corev1.PodSpec{NodeName: "node-a"},
		Status: corev1.PodStatus{
			Phase:      corev1.PodFailed,
			Conditions: conditions,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "step-simple-step",
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ExitCode:   exitCode,
					FinishedAt: metav1.NewTime(testClock.Now().Add(-finishedAgo)),
				}},
			}},
		},
	}
}

func evictedSpotPod() *corev1.Pod {
	pod := failedSpotPod(137, 5*time.Second, corev1.PodCondition{
		Type:    corev1.DisruptionTarget,
		Status:  corev1.ConditionTrue,
		Reason:  corev1.PodReasonTerminationByKubelet,
		Message: "The node was low on resource: memory.",
	})
	pod.Status.Reason = "Evicted"
	pod.Status.Message = "The node was low on resource: memory."
	return pod
}

// gkeRemovedNodePod returns the attempt's pod as GKE leaves it when it removes the node:
// phase Failed with the step still running, and no message.
func gkeRemovedNodePod() *corev1.Pod {
	pod := runningSpotPod()
	pod.Status.Phase = corev1.PodFailed
	pod.Status.Conditions = []corev1.PodCondition{{
		Type:    corev1.DisruptionTarget,
		Status:  corev1.ConditionTrue,
		Reason:  "DeletionByGCPControllerManager",
		Message: "GCPControllerManager: node no longer exists",
	}}
	return pod
}

// runningSpotPod returns the attempt's pod with its step running. It is marked ready, so reconcile does not patch it.
func runningSpotPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: spotRetryPodName, Namespace: "foo", Annotations: map[string]string{"tekton.dev/ready": "READY"}},
		Spec:       corev1.PodSpec{NodeName: "node-a"},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "step-simple-step",
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
}

func imagePullSpotPod() *corev1.Pod {
	pod := runningSpotPod()
	pod.Status.Phase = corev1.PodPending
	pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
		Reason:  "ImagePullBackOff",
		Message: `Back-off pulling image "foo"`,
	}}
	return pod
}

const podRejectedMessage = "Pod was rejected: Node didn't have enough resource: cpu, requested: 4500, used: 27389, capacity: 31850"

// rejectedSpotPod returns the attempt's pod as the kubelet leaves it after it rejected the pod rejectedAgo before the test clock.
func rejectedSpotPod(rejectedAgo time.Duration) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: spotRetryPodName, Namespace: "foo"},
		Spec:       corev1.PodSpec{NodeName: "node-a"},
		Status: corev1.PodStatus{
			Phase:     corev1.PodFailed,
			Reason:    "OutOfcpu",
			Message:   podRejectedMessage,
			StartTime: &metav1.Time{Time: testClock.Now().Add(-rejectedAgo)},
		},
	}
}

func succeeded(status corev1.ConditionStatus, reason, message string) apis.Condition {
	return apis.Condition{Type: apis.ConditionSucceeded, Status: status, Reason: reason, Message: message}
}

// earlierNodeLoss is the condition withNodeLostRetries puts on each earlier attempt.
var earlierNodeLoss = succeeded(corev1.ConditionFalse, reasonNodeLost, "pod lost to node shutdown on node-b")

// withNodeLostRetries gives tr n earlier attempts lost to a node and the matching label.
func withNodeLostRetries(tr *v1.TaskRun, n int) {
	tr.Labels = map[string]string{nodeLostRetriesLabel: strconv.Itoa(n)}
	for range n {
		lostAttempt := v1.TaskRunStatus{}
		lostAttempt.SetCondition(&earlierNodeLoss)
		tr.Status.RetriesStatus = append(tr.Status.RetriesStatus, lostAttempt)
	}
}

// earlierRejection is the condition withRejections puts on each earlier attempt.
var earlierRejection = succeeded(corev1.ConditionFalse, reasonPodRejected, podRejectedMessage)

// withRejections gives tr n earlier attempts whose pod the kubelet rejected.
func withRejections(tr *v1.TaskRun, n int) {
	for range n {
		rejectedAttempt := v1.TaskRunStatus{}
		rejectedAttempt.SetCondition(&earlierRejection)
		tr.Status.RetriesStatus = append(tr.Status.RetriesStatus, rejectedAttempt)
	}
}

func repeatCondition(condition apis.Condition, n int) []apis.Condition {
	conditions := make([]apis.Condition, n)
	for i := range conditions {
		conditions[i] = condition
	}
	return conditions
}

// retryEvents drains the recorder and returns only the NodeLostRetry and PodRejectedRetry events.
func retryEvents(events chan string) []string {
	var freeRetryEvents []string
	for {
		select {
		case event := <-events:
			if strings.HasPrefix(event, "Normal NodeLostRetry ") || strings.HasPrefix(event, "Normal PodRejectedRetry ") {
				freeRetryEvents = append(freeRetryEvents, event)
			}
		default:
			return freeRetryEvents
		}
	}
}

func spotRetryTestData(tr *v1.TaskRun, pod *corev1.Pod, flag string) test.Data {
	data := test.Data{
		TaskRuns: []*v1.TaskRun{tr},
		Tasks:    []*v1.Task{simpleTask},
		ConfigMaps: []*corev1.ConfigMap{{
			ObjectMeta: metav1.ObjectMeta{Namespace: system.Namespace(), Name: config.GetFeatureFlagsConfigName()},
			Data:       map[string]string{"enable-spot-retries": flag},
		}},
	}
	if pod != nil {
		data.Pods = []*corev1.Pod{pod}
	}
	return data
}

// reconcileSpotRetry runs one reconcile and returns the stored TaskRun and whether the reconcile asked for a hold.
func reconcileSpotRetry(t *testing.T, assets test.Assets, tr *v1.TaskRun, wantReconcileError bool) (*v1.TaskRun, bool) {
	t.Helper()
	err := assets.Controller.Reconciler.Reconcile(assets.Ctx, getRunName(tr))
	isRequeue, requeueAfter := controller.IsRequeueKey(err)
	if gotError := err != nil && !isRequeue; gotError != wantReconcileError {
		t.Errorf("Reconcile() = %v, want error %t", err, wantReconcileError)
	}
	stored, err := assets.Clients.Pipeline.TektonV1().TaskRuns(tr.Namespace).Get(assets.Ctx, tr.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting TaskRun: %v", err)
	}
	return stored, isRequeue && requeueAfter == failureCauseRequeue
}

func checkSpotRetryResult(t *testing.T, got *v1.TaskRun, want apis.Condition, wantRetries []apis.Condition, wantLabel string) {
	t.Helper()
	if d := cmp.Diff(&want, got.Status.GetCondition(apis.ConditionSucceeded), ignoreLastTransitionTime); d != "" {
		t.Errorf("condition %s", diff.PrintWantGot(d))
	}
	var gotRetries []apis.Condition
	for _, attempt := range got.Status.RetriesStatus {
		gotRetries = append(gotRetries, *attempt.GetCondition(apis.ConditionSucceeded))
	}
	if d := cmp.Diff(wantRetries, gotRetries, ignoreLastTransitionTime, cmpopts.EquateEmpty()); d != "" {
		t.Errorf("retriesStatus conditions %s", diff.PrintWantGot(d))
	}
	if gotLabel := got.Labels["ci.nomagic.ai/node-lost-retries"]; gotLabel != wantLabel {
		t.Errorf("label ci.nomagic.ai/node-lost-retries = %q, want %q", gotLabel, wantLabel)
	}
}

func TestReconcileSpotRetry(t *testing.T) {
	tests := []struct {
		name               string
		flag               string // enable-spot-retries; "true" when empty
		setup              func(tr *v1.TaskRun)
		pod                *corev1.Pod
		want               apis.Condition
		wantRetries        []apis.Condition // the Succeeded condition of each retriesStatus entry
		wantLabel          string
		wantEvent          string // the NodeLostRetry event, if any
		wantHold           bool
		wantReconcileError bool
		stopSidecarsFails  bool // reading the pod fails, so stopSidecars marks the TaskRun failed
	}{{
		name: "node shutdown, retries 0",
		pod:  failedSpotPod(255, 5*time.Second, nodeShutdownCondition),
		want: succeeded(corev1.ConditionUnknown, v1.TaskRunReasonToBeRetried.String(), "Retrying after node loss (1 of 3): "+nodeShutdownCause),
		wantRetries: []apis.Condition{
			succeeded(corev1.ConditionFalse, reasonNodeLost, nodeShutdownCause+" (was StepFailed: "+stepFailed255+")"),
		},
		wantLabel: "1",
		wantEvent: "Normal NodeLostRetry Retrying after node loss (1 of 3): " + nodeShutdownCause,
	}, {
		name: "node removed by GKE, fixture, retries 1",
		setup: func(tr *v1.TaskRun) {
			tr.Annotations = fixtureAnnotations()
			tr.Spec.Retries = 1
		},
		pod:  gkeRemovedNodePod(),
		want: succeeded(corev1.ConditionUnknown, v1.TaskRunReasonToBeRetried.String(), "Retrying after node loss (1 of 3): "+gkeRemovedNodeCause),
		wantRetries: []apis.Condition{
			succeeded(corev1.ConditionFalse, reasonNodeLost, gkeRemovedNodeCause+" (was Failed: build failed for unspecified reasons.)"),
		},
		wantLabel: "1",
		wantEvent: "Normal NodeLostRetry Retrying after node loss (1 of 3): " + gkeRemovedNodeCause,
	}, {
		name: "eviction is not a node-loss retry",
		pod:  evictedSpotPod(),
		want: succeeded(corev1.ConditionFalse, v1.TaskRunReasonPodEvicted.String(), "The node was low on resource: memory."),
	}, {
		name: "test failure, retries 0",
		pod:  failedSpotPod(1, 5*time.Second),
		want: succeeded(corev1.ConditionFalse, v1.TaskRunReasonStepFailed.String(), stepFailed1),
	}, {
		name:  "test failure, retries 1",
		setup: func(tr *v1.TaskRun) { tr.Spec.Retries = 1 },
		pod:   failedSpotPod(1, 5*time.Second),
		want:  succeeded(corev1.ConditionUnknown, v1.TaskRunReasonToBeRetried.String(), stepFailed1),
		wantRetries: []apis.Condition{
			succeeded(corev1.ConditionFalse, v1.TaskRunReasonStepFailed.String(), stepFailed1),
		},
	}, {
		name:        "limit reached",
		setup:       func(tr *v1.TaskRun) { withNodeLostRetries(tr, 3) },
		pod:         failedSpotPod(255, 5*time.Second, nodeShutdownCondition),
		want:        succeeded(corev1.ConditionFalse, reasonNodeLost, nodeShutdownCause+"; node loss retry limit 3 reached (was StepFailed: "+stepFailed255+")"),
		wantRetries: repeatCondition(earlierNodeLoss, 3),
		wantLabel:   "3",
	}, {
		name: "node-loss retries do not use retries",
		setup: func(tr *v1.TaskRun) {
			withNodeLostRetries(tr, 1)
			tr.Spec.Retries = 1
		},
		pod:  failedSpotPod(1, 5*time.Second),
		want: succeeded(corev1.ConditionUnknown, v1.TaskRunReasonToBeRetried.String(), stepFailed1),
		wantRetries: []apis.Condition{
			earlierNodeLoss,
			succeeded(corev1.ConditionFalse, v1.TaskRunReasonStepFailed.String(), stepFailed1),
		},
		wantLabel: "1",
	}, {
		name:     "rejected pod waits before the retry",
		pod:      rejectedSpotPod(5 * time.Second),
		want:     succeeded(corev1.ConditionUnknown, reasonWaitingAfterRejection, "Waiting to retry rejected pod "+spotRetryPodName),
		wantHold: true,
	}, {
		name:        "rejected pod, retries 0",
		pod:         rejectedSpotPod(rejectedRetryDelay),
		want:        succeeded(corev1.ConditionUnknown, v1.TaskRunReasonToBeRetried.String(), "Retrying rejected pod (1 of 3): "+podRejectedMessage),
		wantRetries: []apis.Condition{earlierRejection},
		wantEvent:   "Normal PodRejectedRetry Retrying rejected pod (1 of 3): " + podRejectedMessage,
	}, {
		name: "rejected pod after the hold",
		setup: func(tr *v1.TaskRun) {
			tr.Status.MarkResourceOngoing(reasonWaitingAfterRejection, "Waiting to retry rejected pod "+spotRetryPodName)
		},
		pod:         rejectedSpotPod(rejectedRetryDelay),
		want:        succeeded(corev1.ConditionUnknown, v1.TaskRunReasonToBeRetried.String(), "Retrying rejected pod (1 of 3): "+podRejectedMessage),
		wantRetries: []apis.Condition{earlierRejection},
		wantEvent:   "Normal PodRejectedRetry Retrying rejected pod (1 of 3): " + podRejectedMessage,
	}, {
		name: "rejected pod without a start time retries now",
		pod: func() *corev1.Pod {
			pod := rejectedSpotPod(0)
			pod.Status.StartTime = nil
			return pod
		}(),
		want:        succeeded(corev1.ConditionUnknown, v1.TaskRunReasonToBeRetried.String(), "Retrying rejected pod (1 of 3): "+podRejectedMessage),
		wantRetries: []apis.Condition{earlierRejection},
		wantEvent:   "Normal PodRejectedRetry Retrying rejected pod (1 of 3): " + podRejectedMessage,
	}, {
		name:        "rejected fixture pod does not wait for a verdict",
		setup:       func(tr *v1.TaskRun) { tr.Annotations = fixtureAnnotations() },
		pod:         rejectedSpotPod(rejectedRetryDelay),
		want:        succeeded(corev1.ConditionUnknown, v1.TaskRunReasonToBeRetried.String(), "Retrying rejected pod (1 of 3): "+podRejectedMessage),
		wantRetries: []apis.Condition{earlierRejection},
		wantEvent:   "Normal PodRejectedRetry Retrying rejected pod (1 of 3): " + podRejectedMessage,
	}, {
		name: "rejections do not use retries",
		setup: func(tr *v1.TaskRun) {
			withRejections(tr, 1)
			tr.Spec.Retries = 1
		},
		pod:  failedSpotPod(1, 5*time.Second),
		want: succeeded(corev1.ConditionUnknown, v1.TaskRunReasonToBeRetried.String(), stepFailed1),
		wantRetries: []apis.Condition{
			earlierRejection,
			succeeded(corev1.ConditionFalse, v1.TaskRunReasonStepFailed.String(), stepFailed1),
		},
	}, {
		name:        "rejection limit reached",
		setup:       func(tr *v1.TaskRun) { withRejections(tr, 3) },
		pod:         rejectedSpotPod(rejectedRetryDelay),
		want:        succeeded(corev1.ConditionFalse, v1.TaskRunReasonFailed.String(), podRejectedMessage),
		wantRetries: repeatCondition(earlierRejection, 3),
	}, {
		name: "rejected pod, flag off",
		flag: "false",
		pod:  rejectedSpotPod(rejectedRetryDelay),
		want: succeeded(corev1.ConditionFalse, v1.TaskRunReasonFailed.String(), podRejectedMessage),
	}, {
		name:     "hold",
		setup:    func(tr *v1.TaskRun) { tr.Annotations = fixtureAnnotations() },
		pod:      failedSpotPod(1, 5*time.Second),
		want:     succeeded(corev1.ConditionUnknown, reasonWaitingForFailureCause, "Waiting for the failure cause of pod "+spotRetryPodName),
		wantHold: true,
	}, {
		name: "verdict node-lost",
		setup: func(tr *v1.TaskRun) {
			tr.Annotations = map[string]string{fixtureAnnotation: "abb-studio-1", failureCauseAnnotation: spotRetryPodName + "=node-lost"}
		},
		pod:  failedSpotPod(1, 5*time.Second),
		want: succeeded(corev1.ConditionUnknown, v1.TaskRunReasonToBeRetried.String(), "Retrying after node loss (1 of 3): VC node lost (operator verdict)"),
		wantRetries: []apis.Condition{
			succeeded(corev1.ConditionFalse, reasonNodeLost, "VC node lost (operator verdict) (was StepFailed: "+stepFailed1+")"),
		},
		wantLabel: "1",
		wantEvent: "Normal NodeLostRetry Retrying after node loss (1 of 3): VC node lost (operator verdict)",
	}, {
		name: "verdict node-lost with the operator's cause",
		setup: func(tr *v1.TaskRun) {
			tr.Annotations = map[string]string{
				fixtureAnnotation:                    "abb-studio-1",
				failureCauseAnnotation:               spotRetryPodName + "=node-lost",
				"ci.nomagic.ai/failure-cause-detail": "VC pod evicted by taint manager on gke-853a87-f442",
			}
		},
		pod:  failedSpotPod(1, 5*time.Second),
		want: succeeded(corev1.ConditionUnknown, v1.TaskRunReasonToBeRetried.String(), "Retrying after node loss (1 of 3): VC pod evicted by taint manager on gke-853a87-f442 (operator verdict)"),
		wantRetries: []apis.Condition{
			succeeded(corev1.ConditionFalse, reasonNodeLost, "VC pod evicted by taint manager on gke-853a87-f442 (operator verdict) (was StepFailed: "+stepFailed1+")"),
		},
		wantLabel: "1",
		wantEvent: "Normal NodeLostRetry Retrying after node loss (1 of 3): VC pod evicted by taint manager on gke-853a87-f442 (operator verdict)",
	}, {
		name: "stale verdict after window",
		setup: func(tr *v1.TaskRun) {
			tr.Annotations = map[string]string{fixtureAnnotation: "abb-studio-1", failureCauseAnnotation: "old-pod=node-lost"}
		},
		pod:  failedSpotPod(1, 60*time.Second),
		want: succeeded(corev1.ConditionFalse, v1.TaskRunReasonStepFailed.String(), stepFailed1),
	}, {
		name: "verdict other",
		setup: func(tr *v1.TaskRun) {
			tr.Annotations = map[string]string{fixtureAnnotation: "abb-studio-1", failureCauseAnnotation: spotRetryPodName + "=other"}
		},
		pod:  failedSpotPod(1, 5*time.Second),
		want: succeeded(corev1.ConditionFalse, v1.TaskRunReasonStepFailed.String(), stepFailed1),
	}, {
		name: "cancel during hold",
		setup: func(tr *v1.TaskRun) {
			tr.Annotations = fixtureAnnotations()
			tr.Spec.Status = v1.TaskRunSpecStatusCancelled
			tr.Status.SetCondition(&apis.Condition{
				Type:    apis.ConditionSucceeded,
				Status:  corev1.ConditionUnknown,
				Reason:  reasonWaitingForFailureCause,
				Message: "Waiting for the failure cause of pod " + spotRetryPodName,
			})
		},
		pod: failedSpotPod(255, 5*time.Second, nodeShutdownCondition),
		want: succeeded(corev1.ConditionFalse, v1.TaskRunReasonCancelled.String(),
			`TaskRun "test-spot-retry" was cancelled. `+"\nPrevious status: [WaitingForFailureCause] Waiting for the failure cause of pod "+spotRetryPodName),
	}, {
		name: "timeout is not a node-loss retry",
		setup: func(tr *v1.TaskRun) {
			tr.Spec.Timeout = &metav1.Duration{Duration: 10 * time.Second}
		},
		pod:  runningSpotPod(),
		want: succeeded(corev1.ConditionFalse, v1.TaskRunReasonTimedOut.String(), `TaskRun "test-spot-retry" failed to finish within "10s"`),
	}, {
		name: "fixture image pull failure does not hold",
		setup: func(tr *v1.TaskRun) {
			tr.Annotations = fixtureAnnotations()
			tr.Status.Steps = []v1.StepState{{
				Name:      "simple-step",
				Container: "step-simple-step",
				ImageID:   "foo",
				ContainerState: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
					Reason:  "ImagePullBackOff",
					Message: `Back-off pulling image "foo"`,
				}},
			}}
		},
		pod: imagePullSpotPod(),
		want: succeeded(corev1.ConditionFalse, v1.TaskRunReasonImagePullFailed.String(),
			`the step "simple-step" in TaskRun "test-spot-retry" failed to pull the image "foo". The pod errored with the message: "Back-off pulling image "foo"."`),
	}, {
		name: "fixture timeout does not hold",
		setup: func(tr *v1.TaskRun) {
			tr.Annotations = fixtureAnnotations()
			tr.Spec.Timeout = &metav1.Duration{Duration: 10 * time.Second}
		},
		pod:  runningSpotPod(),
		want: succeeded(corev1.ConditionFalse, v1.TaskRunReasonTimedOut.String(), `TaskRun "test-spot-retry" failed to finish within "10s"`),
	}, {
		name: "timeout with pod gone",
		setup: func(tr *v1.TaskRun) {
			tr.Spec.Timeout = &metav1.Duration{Duration: 10 * time.Second}
		},
		want: succeeded(corev1.ConditionFalse, v1.TaskRunReasonTimedOut.String(), `TaskRun "test-spot-retry" failed to finish within "10s"`),
	}, {
		name: "fails before any pod exists",
		setup: func(tr *v1.TaskRun) {
			tr.Spec.TaskRef.Name = "missing-task"
			tr.Status = v1.TaskRunStatus{}
			// A verdict without a pod name must not match an attempt that never had a pod.
			tr.Annotations = map[string]string{fixtureAnnotation: "abb-studio-1", failureCauseAnnotation: "=node-lost"}
		},
		want: succeeded(corev1.ConditionFalse, "TaskRunResolutionFailed",
			`error when listing tasks for taskRun test-spot-retry: tasks.tekton.dev "missing-task" not found`),
		wantReconcileError: true,
	}, {
		name: "flag off",
		flag: "false",
		pod:  failedSpotPod(255, 5*time.Second, nodeShutdownCondition),
		want: succeeded(corev1.ConditionFalse, v1.TaskRunReasonStepFailed.String(), stepFailed255),
	}, {
		name:  "flag off keeps upstream retries",
		flag:  "false",
		setup: func(tr *v1.TaskRun) { tr.Spec.Retries = 1 },
		pod:   failedSpotPod(255, 5*time.Second, nodeShutdownCondition),
		want:  succeeded(corev1.ConditionUnknown, v1.TaskRunReasonToBeRetried.String(), stepFailed255),
		wantRetries: []apis.Condition{
			succeeded(corev1.ConditionFalse, v1.TaskRunReasonStepFailed.String(), stepFailed255),
		},
	}, {
		name: "done TaskRun is not revived",
		setup: func(tr *v1.TaskRun) {
			tr.Status.MarkResourceFailed(v1.TaskRunReasonStepFailed, errors.New(stepFailed255))
		},
		pod:  failedSpotPod(255, 5*time.Second, nodeShutdownCondition),
		want: succeeded(corev1.ConditionFalse, v1.TaskRunReasonStepFailed.String(), stepFailed255),
	}, {
		name: "final NodeLost is not rewritten",
		setup: func(tr *v1.TaskRun) {
			withNodeLostRetries(tr, 3)
			tr.Status.MarkResourceFailed(reasonNodeLost, errors.New(nodeShutdownCause+"; node loss retry limit 3 reached (was StepFailed: "+stepFailed255+")"))
		},
		pod:         failedSpotPod(255, 5*time.Second, nodeShutdownCondition),
		want:        succeeded(corev1.ConditionFalse, reasonNodeLost, nodeShutdownCause+"; node loss retry limit 3 reached (was StepFailed: "+stepFailed255+")"),
		wantRetries: repeatCondition(earlierNodeLoss, 3),
		wantLabel:   "3",
	}, {
		name: "succeeded TaskRun gets no node-loss retry",
		setup: func(tr *v1.TaskRun) {
			tr.Status.SetCondition(&apis.Condition{
				Type:    apis.ConditionSucceeded,
				Status:  corev1.ConditionTrue,
				Reason:  v1.TaskRunReasonSuccessful.String(),
				Message: "All Steps have completed executing",
			})
		},
		pod:               failedSpotPod(255, 5*time.Second, nodeShutdownCondition),
		stopSidecarsFails: true,
		want: succeeded(corev1.ConditionFalse, v1.TaskRunReasonStopSidecarFailed.String(),
			`error getting Pod "test-spot-retry-pod" when stopping sidecars: etcdserver: request timed out`),
	}, {
		name: "lost label is repaired",
		setup: func(tr *v1.TaskRun) {
			withNodeLostRetries(tr, 1)
			tr.Labels = nil // the label write failed when the node-loss retry happened
		},
		pod:         runningSpotPod(),
		want:        succeeded(corev1.ConditionUnknown, v1.TaskRunReasonRunning.String(), "Not all Steps in the Task have finished executing"),
		wantRetries: []apis.Condition{earlierNodeLoss},
		wantLabel:   "1",
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tr := spotRetryTaskRun(t)
			if tc.setup != nil {
				tc.setup(tr)
			}
			flag := tc.flag
			if flag == "" {
				flag = "true"
			}
			assets, cancel := getTaskRunController(t, spotRetryTestData(tr, tc.pod, flag))
			defer cancel()
			createServiceAccount(t, assets, "default", tr.Namespace)
			if tc.stopSidecarsFails {
				assets.Clients.Kube.PrependReactor("get", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
					return true, nil, errors.New("etcdserver: request timed out")
				})
			}

			got, gotHold := reconcileSpotRetry(t, assets, tr, tc.wantReconcileError)

			if gotHold != tc.wantHold {
				t.Errorf("hold = %t, want %t", gotHold, tc.wantHold)
			}
			if tc.wantHold && got.Status.CompletionTime != nil {
				t.Errorf("CompletionTime = %v, want nil during a hold", got.Status.CompletionTime)
			}
			checkSpotRetryResult(t, got, tc.want, tc.wantRetries, tc.wantLabel)
			if provenance := got.Status.Provenance; provenance != nil && provenance.FeatureFlags.EnableSpotRetries {
				t.Error("status provenance has EnableSpotRetries, want it left out")
			}
			var wantEvents []string
			if tc.wantEvent != "" {
				wantEvents = []string{tc.wantEvent}
			}
			if d := cmp.Diff(wantEvents, retryEvents(assets.Recorder.Events), cmpopts.EquateEmpty()); d != "" {
				t.Errorf("retry events %s", diff.PrintWantGot(d))
			}
		})
	}
}

// TestReconcileSpotRetryAfterHold checks that the reconcile after a hold reads the failure again and decides.
func TestReconcileSpotRetryAfterHold(t *testing.T) {
	tests := []struct {
		name        string
		verdict     string // the failure-cause annotation written during the hold
		clockStep   time.Duration
		want        apis.Condition
		wantRetries []apis.Condition
		wantLabel   string
	}{{
		name:    "verdict arrives",
		verdict: spotRetryPodName + "=node-lost",
		want:    succeeded(corev1.ConditionUnknown, v1.TaskRunReasonToBeRetried.String(), "Retrying after node loss (1 of 3): VC node lost (operator verdict)"),
		wantRetries: []apis.Condition{
			succeeded(corev1.ConditionFalse, reasonNodeLost, "VC node lost (operator verdict) (was StepFailed: "+stepFailed1+")"),
		},
		wantLabel: "1",
	}, {
		name:      "no verdict within the window",
		clockStep: failureCauseWait,
		want:      succeeded(corev1.ConditionFalse, v1.TaskRunReasonStepFailed.String(), stepFailed1),
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tr := spotRetryTaskRun(t)
			tr.Annotations = fixtureAnnotations()
			assets, cancel := getTaskRunController(t, spotRetryTestData(tr, failedSpotPod(1, 5*time.Second), "true"))
			defer cancel()
			createServiceAccount(t, assets, "default", tr.Namespace)

			held, gotHold := reconcileSpotRetry(t, assets, tr, false)
			if !gotHold {
				t.Fatalf("first reconcile did not hold, condition %v", held.Status.GetCondition(apis.ConditionSucceeded))
			}

			if tc.verdict != "" {
				held.Annotations[failureCauseAnnotation] = tc.verdict
				var err error
				if held, err = assets.Clients.Pipeline.TektonV1().TaskRuns(tr.Namespace).Update(assets.Ctx, held, metav1.UpdateOptions{}); err != nil {
					t.Fatalf("writing the verdict: %v", err)
				}
			}
			originalTime := testClock.Now()
			defer testClock.SetTime(originalTime)
			testClock.SetTime(originalTime.Add(tc.clockStep))
			if err := assets.Informers.TaskRun.Informer().GetIndexer().Add(held); err != nil {
				t.Fatal(err)
			}

			got, gotHold := reconcileSpotRetry(t, assets, tr, false)
			if gotHold {
				t.Errorf("second reconcile held again")
			}
			checkSpotRetryResult(t, got, tc.want, tc.wantRetries, tc.wantLabel)
		})
	}
}

// heldTaskRun returns a fixture TaskRun that holds for the verdict. Its step finished finishedAgo before the test clock.
func heldTaskRun(t *testing.T, finishedAgo time.Duration) *v1.TaskRun {
	t.Helper()
	tr := spotRetryTaskRun(t)
	tr.Annotations = fixtureAnnotations()
	tr.Status.MarkResourceOngoing(reasonWaitingForFailureCause, "Waiting for the failure cause of pod "+spotRetryPodName)
	tr.Status.Steps = []v1.StepState{{
		Name:      "simple-step",
		Container: "step-simple-step",
		ContainerState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode:   1,
			FinishedAt: metav1.NewTime(testClock.Now().Add(-finishedAgo)),
		}},
	}}
	return tr
}

// createdPods returns the names of the pods created through the fake kube client.
func createdPods(assets test.Assets) []string {
	var names []string
	for _, action := range assets.Clients.Kube.Actions() {
		if !action.Matches("create", "pods") {
			continue
		}
		names = append(names, action.(ktesting.CreateAction).GetObject().(*corev1.Pod).Name)
	}
	return names
}

// TestReconcileSpotRetryPodDeletedDuringHold checks that a pod deleted during a hold is never created again.
// The attempt is decided from status.steps instead.
func TestReconcileSpotRetryPodDeletedDuringHold(t *testing.T) {
	deletedMessage := "pod " + spotRetryPodName + " was deleted while waiting for the failure cause"
	tests := []struct {
		name        string
		finishedAgo time.Duration
		verdict     string // the failure-cause annotation
		want        apis.Condition
		wantRetries []apis.Condition
		wantLabel   string
		wantHold    bool
		wantNextPod string // the pod the next reconcile creates, if any
	}{{
		name:        "within window",
		finishedAgo: 10 * time.Second,
		want:        succeeded(corev1.ConditionUnknown, reasonWaitingForFailureCause, "Waiting for the failure cause of pod "+spotRetryPodName),
		wantHold:    true,
	}, {
		name:        "window over",
		finishedAgo: 60 * time.Second,
		want:        succeeded(corev1.ConditionFalse, v1.TaskRunReasonFailed.String(), deletedMessage),
	}, {
		name:        "window over, verdict node-lost",
		finishedAgo: 60 * time.Second,
		verdict:     spotRetryPodName + "=node-lost",
		want:        succeeded(corev1.ConditionUnknown, v1.TaskRunReasonToBeRetried.String(), "Retrying after node loss (1 of 3): VC node lost (operator verdict)"),
		wantRetries: []apis.Condition{
			succeeded(corev1.ConditionFalse, reasonNodeLost, "VC node lost (operator verdict) (was Failed: "+deletedMessage+")"),
		},
		wantLabel:   "1",
		wantNextPod: "test-spot-retry-pod-retry1",
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tr := heldTaskRun(t, tc.finishedAgo)
			if tc.verdict != "" {
				tr.Annotations[failureCauseAnnotation] = tc.verdict
			}
			assets, cancel := getTaskRunController(t, spotRetryTestData(tr, nil, "true"))
			defer cancel()
			createServiceAccount(t, assets, "default", tr.Namespace)

			got, gotHold := reconcileSpotRetry(t, assets, tr, false)

			if created := createdPods(assets); len(created) > 0 {
				t.Errorf("created pods %v, want none", created)
			}
			if gotHold != tc.wantHold {
				t.Errorf("hold = %t, want %t", gotHold, tc.wantHold)
			}
			checkSpotRetryResult(t, got, tc.want, tc.wantRetries, tc.wantLabel)
			if tc.wantNextPod == "" {
				return
			}

			if err := assets.Informers.TaskRun.Informer().GetIndexer().Update(got); err != nil {
				t.Fatal(err)
			}
			reconcileSpotRetry(t, assets, tr, false)
			if d := cmp.Diff([]string{tc.wantNextPod}, createdPods(assets)); d != "" {
				t.Errorf("created pods %s", diff.PrintWantGot(d))
			}
		})
	}
}

// TestReconcileRejectedPodDeletedDuringHold checks that a rejected pod deleted while it waits for its retry is not created again.
func TestReconcileRejectedPodDeletedDuringHold(t *testing.T) {
	tr := spotRetryTaskRun(t)
	tr.Status.MarkResourceOngoing(reasonWaitingAfterRejection, "Waiting to retry rejected pod "+spotRetryPodName)
	assets, cancel := getTaskRunController(t, spotRetryTestData(tr, nil, "true"))
	defer cancel()
	createServiceAccount(t, assets, "default", tr.Namespace)

	got, gotHold := reconcileSpotRetry(t, assets, tr, false)

	if created := createdPods(assets); len(created) > 0 {
		t.Errorf("created pods %v, want none", created)
	}
	if gotHold {
		t.Error("hold = true, want false")
	}
	want := succeeded(corev1.ConditionFalse, v1.TaskRunReasonFailed.String(), "pod "+spotRetryPodName+" was deleted while waiting for the retry")
	checkSpotRetryResult(t, got, want, nil, "")
}

// TestReconcileSpotRetryStatusWriteFails checks that a rejected status write does not use a node-loss retry,
// although the label write in syncMetadata succeeds.
func TestReconcileSpotRetryStatusWriteFails(t *testing.T) {
	tr := spotRetryTaskRun(t)
	assets, cancel := getTaskRunController(t, spotRetryTestData(tr, failedSpotPod(255, 5*time.Second, nodeShutdownCondition), "true"))
	defer cancel()
	createServiceAccount(t, assets, "default", tr.Namespace)
	assets.Clients.Pipeline.PrependReactor("update", "taskruns", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "status" {
			return false, nil, nil
		}
		return true, nil, errors.New("admission webhook denied the request")
	})

	for range 2 {
		stored, _ := reconcileSpotRetry(t, assets, tr, true)
		if err := assets.Informers.TaskRun.Informer().GetIndexer().Update(stored); err != nil {
			t.Fatal(err)
		}
	}

	stored, err := assets.Clients.Pipeline.TektonV1().TaskRuns(tr.Namespace).Get(assets.Ctx, tr.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if gotLabel := stored.Labels["ci.nomagic.ai/node-lost-retries"]; gotLabel != "1" {
		t.Errorf("label ci.nomagic.ai/node-lost-retries = %q, want \"1\"", gotLabel)
	}
	wantEvent := "Normal NodeLostRetry Retrying after node loss (1 of 3): " + nodeShutdownCause
	if d := cmp.Diff([]string{wantEvent, wantEvent}, retryEvents(assets.Recorder.Events)); d != "" {
		t.Errorf("retry events %s", diff.PrintWantGot(d))
	}
}
