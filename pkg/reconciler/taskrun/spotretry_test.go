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
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	v1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func podWithDisruption(status corev1.ConditionStatus, reason, message string) *corev1.Pod {
	return &corev1.Pod{
		Spec: corev1.PodSpec{NodeName: "node-a"},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{
				Type:    corev1.DisruptionTarget,
				Status:  status,
				Reason:  reason,
				Message: message,
			}},
		},
	}
}

func TestNodeLostCause(t *testing.T) {
	evicted := podWithDisruption(corev1.ConditionTrue, corev1.PodReasonTerminationByKubelet, "The node was low on resource: memory.")
	evicted.Status.Reason = "Evicted"

	tests := []struct {
		name      string
		pod       *corev1.Pod
		wantCause string
		wantLost  bool
	}{{
		name:      "node shutdown",
		pod:       podWithDisruption(corev1.ConditionTrue, corev1.PodReasonTerminationByKubelet, nodeShutdownMessage),
		wantCause: "pod lost to node shutdown on node-a",
		wantLost:  true,
	}, {
		name: "eviction by kubelet is not a lost node",
		pod:  evicted,
	}, {
		name:      "podgc",
		pod:       podWithDisruption(corev1.ConditionTrue, podReasonDeletionByPodGC, "PodGC: node no longer exists"),
		wantCause: "pod deleted by PodGC on node-a",
		wantLost:  true,
	}, {
		name:      "taint manager",
		pod:       podWithDisruption(corev1.ConditionTrue, podReasonDeletionByTaintManager, "Taint manager: deleting due to NoExecute taint"),
		wantCause: "pod evicted by taint manager on node-a",
		wantLost:  true,
	}, {
		name: "condition false",
		pod:  podWithDisruption(corev1.ConditionFalse, corev1.PodReasonTerminationByKubelet, nodeShutdownMessage),
	}, {
		name: "unrelated disruption reason",
		pod:  podWithDisruption(corev1.ConditionTrue, corev1.PodReasonPreemptionByScheduler, "preempted"),
	}, {
		name: "no conditions",
		pod:  &corev1.Pod{Spec: corev1.PodSpec{NodeName: "node-a"}},
	}, {
		name: "nil pod",
		pod:  nil,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cause, lost := nodeLostCause(tc.pod)
			if lost != tc.wantLost {
				t.Errorf("nodeLostCause() lost = %t, want %t", lost, tc.wantLost)
			}
			if d := cmp.Diff(tc.wantCause, cause); d != "" {
				t.Errorf("nodeLostCause() cause %s", d)
			}
		})
	}
}

func TestFailureCauseVerdict(t *testing.T) {
	tests := []struct {
		name         string
		annotations  map[string]string
		wantNodeLost bool
		wantFound    bool
	}{{
		name:         "node lost",
		annotations:  map[string]string{failureCauseAnnotation: "p1=node-lost"},
		wantNodeLost: true,
		wantFound:    true,
	}, {
		name:        "other",
		annotations: map[string]string{failureCauseAnnotation: "p1=other"},
		wantFound:   true,
	}, {
		name:        "verdict for another pod",
		annotations: map[string]string{failureCauseAnnotation: "p0=node-lost"},
	}, {
		name: "missing annotation",
	}, {
		name:        "garbage",
		annotations: map[string]string{failureCauseAnnotation: "garbage"},
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tr := &v1.TaskRun{ObjectMeta: metav1.ObjectMeta{Annotations: tc.annotations}}
			nodeLost, found := failureCauseVerdict(tr, "p1")
			if nodeLost != tc.wantNodeLost || found != tc.wantFound {
				t.Errorf("failureCauseVerdict() = (%t, %t), want (%t, %t)", nodeLost, found, tc.wantNodeLost, tc.wantFound)
			}
		})
	}
}

func TestSpotRetryCount(t *testing.T) {
	attempt := func(reason v1.TaskRunReason) v1.TaskRunStatus {
		status := v1.TaskRunStatus{}
		status.MarkResourceFailed(reason, errors.New("failed"))
		return status
	}

	tests := []struct {
		name    string
		labels  map[string]string
		retries []v1.TaskRunStatus
		want    int
	}{{
		name: "no entries",
		want: 0,
	}, {
		name:    "two node losses and one step failure",
		retries: []v1.TaskRunStatus{attempt(reasonNodeLost), attempt(v1.TaskRunReasonStepFailed), attempt(reasonNodeLost)},
		want:    2,
	}, {
		name:   "the label alone does not count",
		labels: map[string]string{spotRetriesLabel: "3"},
		want:   0,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tr := &v1.TaskRun{
				ObjectMeta: metav1.ObjectMeta{Labels: tc.labels},
				Status:     v1.TaskRunStatus{TaskRunStatusFields: v1.TaskRunStatusFields{RetriesStatus: tc.retries}},
			}
			if got := spotRetryCount(tr); got != tc.want {
				t.Errorf("spotRetryCount() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestNeedsFailureCauseWait(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		reason      v1.TaskRunReason // the Succeeded reason; no condition when empty
		want        bool
	}{{
		name:        "fixture, step failed",
		annotations: map[string]string{fixtureAnnotation: "abb-studio-1"},
		reason:      v1.TaskRunReasonStepFailed,
		want:        true,
	}, {
		name:        "fixture, sidecar failed",
		annotations: map[string]string{fixtureAnnotation: "abb-studio-1"},
		reason:      v1.TaskRunReasonSidecarFailed,
		want:        true,
	}, {
		name:        "fixture, pod failed",
		annotations: map[string]string{fixtureAnnotation: "abb-studio-1"},
		reason:      v1.TaskRunReasonFailed,
		want:        true,
	}, {
		name:        "fixture, timeout made by the reconciler",
		annotations: map[string]string{fixtureAnnotation: "abb-studio-1"},
		reason:      v1.TaskRunReasonTimedOut,
	}, {
		name:        "fixture, image pull failure made by the reconciler",
		annotations: map[string]string{fixtureAnnotation: "abb-studio-1"},
		reason:      v1.TaskRunReasonImagePullFailed,
	}, {
		name:        "fixture, step OOM",
		annotations: map[string]string{fixtureAnnotation: "abb-studio-1"},
		reason:      v1.TaskRunReasonStepOOM,
	}, {
		name:        "fixture, evicted",
		annotations: map[string]string{fixtureAnnotation: "abb-studio-1"},
		reason:      v1.TaskRunReasonPodEvicted,
	}, {
		name:        "fixture, no condition",
		annotations: map[string]string{fixtureAnnotation: "abb-studio-1"},
	}, {
		name:   "no fixture, step failed",
		reason: v1.TaskRunReasonStepFailed,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tr := &v1.TaskRun{ObjectMeta: metav1.ObjectMeta{Annotations: tc.annotations}}
			if tc.reason != "" {
				tr.Status.MarkResourceFailed(tc.reason, errors.New("failed"))
			}
			if got := needsFailureCauseWait(tr); got != tc.want {
				t.Errorf("needsFailureCauseWait() = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestAttemptFailedAt(t *testing.T) {
	finishedAt := func(clock string) corev1.ContainerState {
		finished, err := time.Parse(time.RFC3339, "2026-10-31T"+clock+"Z")
		if err != nil {
			t.Fatal(err)
		}
		return corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{FinishedAt: metav1.NewTime(finished)}}
	}

	tests := []struct {
		name      string
		status    v1.TaskRunStatusFields
		want      time.Time
		wantFound bool
	}{{
		name: "latest of two steps",
		status: v1.TaskRunStatusFields{Steps: []v1.StepState{
			{ContainerState: finishedAt("10:00:00")},
			{ContainerState: finishedAt("10:00:07")},
		}},
		want:      time.Date(2026, 10, 31, 10, 0, 7, 0, time.UTC),
		wantFound: true,
	}, {
		name: "a sidecar finished last",
		status: v1.TaskRunStatusFields{
			Steps:    []v1.StepState{{ContainerState: finishedAt("10:00:00")}},
			Sidecars: []v1.SidecarState{{ContainerState: finishedAt("10:00:09")}},
		},
		want:      time.Date(2026, 10, 31, 10, 0, 9, 0, time.UTC),
		wantFound: true,
	}, {
		name: "no terminated containers",
		status: v1.TaskRunStatusFields{
			Steps:    []v1.StepState{{ContainerState: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}},
			Sidecars: []v1.SidecarState{{}},
		},
	}, {
		name: "zero finish time is ignored",
		status: v1.TaskRunStatusFields{Steps: []v1.StepState{
			{ContainerState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}}},
		}},
	}, {
		name: "no containers",
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tr := &v1.TaskRun{Status: v1.TaskRunStatus{TaskRunStatusFields: tc.status}}
			got, found := attemptFailedAt(tr)
			if found != tc.wantFound {
				t.Errorf("attemptFailedAt() found = %t, want %t", found, tc.wantFound)
			}
			if !got.Equal(tc.want) {
				t.Errorf("attemptFailedAt() = %s, want %s", got, tc.want)
			}
		})
	}
}
