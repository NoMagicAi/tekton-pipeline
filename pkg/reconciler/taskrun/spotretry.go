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
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tektoncd/pipeline/pkg/apis/config"
	v1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/controller"
)

const (
	// maxSpotRetries is how many times a TaskRun is retried after its pod lost its node.
	maxSpotRetries = 3
	// maxRejectedRetries is how many times a TaskRun is retried after the kubelet rejected its pod.
	maxRejectedRetries = 3

	spotRetriesLabel       = "ci.nomagic.ai/spot-retries"
	failureCauseAnnotation = "ci.nomagic.ai/failure-cause"
	fixtureAnnotation      = "nomagic.ai/fixture"

	reasonNodeLost               = "NodeLost"
	reasonWaitingForFailureCause = "WaitingForFailureCause"
	reasonPodRejected            = "PodRejected"
	reasonWaitingAfterRejection  = "WaitingAfterPodRejection"

	// rejectedRetryDelay lets the scheduler see the node as the kubelet does before the next pod.
	// That gap lasted up to 73 s on ci-1, so 3 retries 30 s apart cover it.
	rejectedRetryDelay = 30 * time.Second

	// failureCauseWait is how long a failed attempt waits for the operator's verdict.
	failureCauseWait    = 30 * time.Second
	failureCauseRequeue = 5 * time.Second

	// Kubernetes has no exported constants for these DisruptionTarget reasons and message.
	podReasonDeletionByPodGC        = "DeletionByPodGC"
	podReasonDeletionByTaintManager = "DeletionByTaintManager"
	nodeShutdownMessage             = "Pod was terminated in response to imminent node shutdown."
	// podRejectedPrefix starts the message of every pod the kubelet rejects at admission.
	podRejectedPrefix = "Pod was rejected: "

	verdictNodeLost = "node-lost"
	verdictOther    = "other"

	operatorVerdictCause     = "VC node preempted (operator verdict)"
	eventReasonSpotRetry     = "SpotRetry"
	eventReasonRejectedRetry = "PodRejectedRetry"
)

// retryDecision is what follows a failed attempt when spot retries are on.
type retryDecision int

const (
	retryNone retryDecision = iota
	retrySpot
	retryRejected
	retryUpstream
	retryHold
	retryRejectedHold
)

// nodeLostCause reports whether the pod was disrupted because its node went away.
// An eviction for node pressure is not a lost node.
func nodeLostCause(pod *corev1.Pod) (cause string, lost bool) {
	if pod == nil {
		return "", false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type != corev1.DisruptionTarget || condition.Status != corev1.ConditionTrue {
			continue
		}
		switch {
		case condition.Reason == corev1.PodReasonTerminationByKubelet && condition.Message == nodeShutdownMessage:
			return fmt.Sprintf("pod lost to node shutdown on %s", pod.Spec.NodeName), true
		case condition.Reason == podReasonDeletionByPodGC:
			return fmt.Sprintf("pod deleted by PodGC on %s", pod.Spec.NodeName), true
		case condition.Reason == podReasonDeletionByTaintManager:
			return fmt.Sprintf("pod evicted by taint manager on %s", pod.Spec.NodeName), true
		}
	}
	return "", false
}

// podRejected reports whether the kubelet rejected the pod at admission. No container of such a pod ran.
func podRejected(pod *corev1.Pod) bool {
	return pod != nil && pod.Status.Phase == corev1.PodFailed && strings.HasPrefix(pod.Status.Message, podRejectedPrefix)
}

// failureCauseVerdict reads the operator's verdict ("<pod name>=node-lost" or "=other") for podName.
// found is false when there is no verdict for that pod.
func failureCauseVerdict(tr *v1.TaskRun, podName string) (nodeLost bool, found bool) {
	verdictPod, verdict, _ := strings.Cut(tr.Annotations[failureCauseAnnotation], "=")
	if verdictPod != podName {
		return false, false
	}
	switch verdict {
	case verdictNodeLost:
		return true, true
	case verdictOther:
		return false, true
	}
	return false, false
}

// spotRetryCount returns the number of spot retries used: earlier attempts with the reason NodeLost.
// It reads the status, not the label, because syncMetadata writes the label even when the status write fails.
func spotRetryCount(tr *v1.TaskRun) int {
	return retriesWithReason(tr, reasonNodeLost)
}

// retriesWithReason returns the number of earlier attempts that ended with reason.
func retriesWithReason(tr *v1.TaskRun, reason string) int {
	count := 0
	for _, attempt := range tr.Status.RetriesStatus {
		if attempt.GetCondition(apis.ConditionSucceeded).GetReason() == reason {
			count++
		}
	}
	return count
}

// needsFailureCauseWait reports whether a failed attempt waits for the operator's verdict. Only a fixture
// TaskRun whose pod reported the failure waits; reconciler-made failures may have deleted the pod.
func needsFailureCauseWait(tr *v1.TaskRun) bool {
	if _, found := tr.Annotations[fixtureAnnotation]; !found {
		return false
	}
	condition := tr.Status.GetCondition(apis.ConditionSucceeded)
	if condition == nil {
		return false
	}
	switch v1.TaskRunReason(condition.Reason) {
	case v1.TaskRunReasonStepFailed, v1.TaskRunReasonSidecarFailed, v1.TaskRunReasonFailed:
		return true
	}
	return false
}

// attemptFailedAt returns when the last container of the attempt finished.
// It does not use CompletionTime, which every reconcile resets.
func attemptFailedAt(tr *v1.TaskRun) (time.Time, bool) {
	var failedAt time.Time
	found := false
	record := func(state corev1.ContainerState) {
		if state.Terminated == nil || state.Terminated.FinishedAt.IsZero() {
			return
		}
		finishedAt := state.Terminated.FinishedAt.Time
		if !found || finishedAt.After(failedAt) {
			failedAt, found = finishedAt, true
		}
	}
	for _, step := range tr.Status.Steps {
		record(step.ContainerState)
	}
	for _, sidecar := range tr.Status.Sidecars {
		record(sidecar.ContainerState)
	}
	return failedAt, found
}

// decideRetry picks what follows a newly failed attempt. A non-empty cause means the pod lost its node.
// A pod the kubelet rejected gets a free retry rejectedRetryDelay after the rejection.
func (c *Reconciler) decideRetry(tr *v1.TaskRun) (decision retryDecision, cause string) {
	podName := tr.Status.PodName
	lost := false
	var pod *corev1.Pod
	if podName != "" {
		if found, err := c.podLister.Pods(tr.Namespace).Get(podName); err == nil {
			pod = found
			cause, lost = nodeLostCause(pod)
		}
	}
	rejectedRetries := retriesWithReason(tr, reasonPodRejected)
	if podRejected(pod) && rejectedRetries < maxRejectedRetries {
		if pod.Status.StartTime != nil && c.Clock.Since(pod.Status.StartTime.Time) < rejectedRetryDelay {
			return retryRejectedHold, ""
		}
		return retryRejected, ""
	}
	if podName != "" && !lost && needsFailureCauseWait(tr) {
		nodeLost, found := failureCauseVerdict(tr, podName)
		failedAt, failed := attemptFailedAt(tr)
		switch {
		case nodeLost:
			cause, lost = operatorVerdictCause, true
		case !found && failed && c.Clock.Since(failedAt) < failureCauseWait:
			return retryHold, ""
		}
	}

	spotRetries := spotRetryCount(tr)
	if lost && spotRetries < maxSpotRetries {
		return retrySpot, cause
	}
	if len(tr.Status.RetriesStatus)-spotRetries-rejectedRetries < tr.Spec.Retries {
		return retryUpstream, ""
	}
	return retryNone, cause
}

// retryTaskRunAfterNodeLoss retries tr as a spot retry and marks the lost attempt with the reason NodeLost.
func retryTaskRunAfterNodeLoss(tr *v1.TaskRun, cause string) {
	failed := tr.Status.GetCondition(apis.ConditionSucceeded)
	lostMessage := fmt.Sprintf("%s (was %s: %s)", cause, failed.Reason, failed.Message)
	spotRetries := spotRetryCount(tr) + 1

	retryTaskRun(tr, fmt.Sprintf("Retrying after node loss (%d of %d): %s", spotRetries, maxSpotRetries, cause))
	lostAttempt := &tr.Status.RetriesStatus[len(tr.Status.RetriesStatus)-1]
	lostAttempt.SetCondition(&apis.Condition{
		Type:    apis.ConditionSucceeded,
		Status:  corev1.ConditionFalse,
		Reason:  reasonNodeLost,
		Message: lostMessage,
	})
}

// retryTaskRunAfterRejection retries tr for free and marks the rejected attempt with the reason PodRejected.
func retryTaskRunAfterRejection(tr *v1.TaskRun) {
	rejectedMessage := tr.Status.GetCondition(apis.ConditionSucceeded).Message
	rejectedRetries := retriesWithReason(tr, reasonPodRejected) + 1

	retryTaskRun(tr, fmt.Sprintf("Retrying rejected pod (%d of %d): %s", rejectedRetries, maxRejectedRetries, rejectedMessage))
	rejectedAttempt := &tr.Status.RetriesStatus[len(tr.Status.RetriesStatus)-1]
	rejectedAttempt.SetCondition(&apis.Condition{
		Type:    apis.ConditionSucceeded,
		Status:  corev1.ConditionFalse,
		Reason:  reasonPodRejected,
		Message: rejectedMessage,
	})
}

// setSpotRetriesLabel mirrors the spot retry count into the label. It runs on every reconcile,
// so it also repairs a label write that failed at the spot retry.
func setSpotRetriesLabel(ctx context.Context, tr *v1.TaskRun) {
	if !config.FromContextOrDefaults(ctx).FeatureFlags.EnableSpotRetries {
		return
	}
	spotRetries := spotRetryCount(tr)
	if spotRetries == 0 {
		return
	}
	if tr.Labels == nil {
		tr.Labels = map[string]string{}
	}
	tr.Labels[spotRetriesLabel] = strconv.Itoa(spotRetries)
}

// failHoldWithoutPod fails a held attempt whose pod is gone, so reconcile does not create the pod again
// and rerun the attempt. The failure is then decided like any other. It reports whether it failed tr.
func (c *Reconciler) failHoldWithoutPod(ctx context.Context, tr *v1.TaskRun) bool {
	if !config.FromContextOrDefaults(ctx).FeatureFlags.EnableSpotRetries {
		return false
	}
	var waitingFor string
	switch tr.Status.GetCondition(apis.ConditionSucceeded).GetReason() {
	case reasonWaitingForFailureCause:
		waitingFor = "the failure cause"
	case reasonWaitingAfterRejection:
		waitingFor = "the retry"
	default:
		return false
	}
	if _, err := c.podLister.Pods(tr.Namespace).Get(tr.Status.PodName); !k8serrors.IsNotFound(err) {
		return false
	}
	tr.Status.MarkResourceFailed(v1.TaskRunReasonFailed, fmt.Errorf("pod %s was deleted while waiting for %s", tr.Status.PodName, waitingFor))
	return true
}

// retryAfterFailure applies decideRetry to a newly failed attempt. It reports whether to hold for the verdict.
func (c *Reconciler) retryAfterFailure(ctx context.Context, tr *v1.TaskRun) (hold bool) {
	decision, cause := c.decideRetry(tr)
	failed := tr.Status.GetCondition(apis.ConditionSucceeded)
	switch decision {
	case retrySpot:
		retryTaskRunAfterNodeLoss(tr, cause)
		message := tr.Status.GetCondition(apis.ConditionSucceeded).Message
		controller.GetEventRecorder(ctx).Event(tr, corev1.EventTypeNormal, eventReasonSpotRetry, message)
	case retryRejected:
		retryTaskRunAfterRejection(tr)
		message := tr.Status.GetCondition(apis.ConditionSucceeded).Message
		controller.GetEventRecorder(ctx).Event(tr, corev1.EventTypeNormal, eventReasonRejectedRetry, message)
	case retryUpstream:
		retryTaskRun(tr, failed.Message)
	case retryHold:
		tr.Status.MarkResourceOngoing(reasonWaitingForFailureCause, "Waiting for the failure cause of pod "+tr.Status.PodName)
		tr.Status.CompletionTime = nil
		return true
	case retryRejectedHold:
		tr.Status.MarkResourceOngoing(reasonWaitingAfterRejection, "Waiting to retry rejected pod "+tr.Status.PodName)
		tr.Status.CompletionTime = nil
		return true
	case retryNone:
		if cause != "" {
			tr.Status.SetCondition(&apis.Condition{
				Type:    apis.ConditionSucceeded,
				Status:  corev1.ConditionFalse,
				Reason:  reasonNodeLost,
				Message: fmt.Sprintf("%s; spot retry limit %d reached (was %s: %s)", cause, maxSpotRetries, failed.Reason, failed.Message),
			})
		}
	}
	return false
}
