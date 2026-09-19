package apiservicehealer

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	apiregistrationclient "github.com/rancher/wrangler/v3/pkg/generated/controllers/apiregistration.k8s.io/v1"
	coreclient "github.com/rancher/wrangler/v3/pkg/generated/controllers/core/v1"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
)

// Healer is the background controller that monitors terminating namespaces and safely
// unblocks deletion deadlocks caused by unresponsive non-storable virtual APIServices.
type Healer struct {
	ctx         context.Context
	namespaces  coreclient.NamespaceController
	apiServices apiregistrationclient.APIServiceController
	k8sClient   kubernetes.Interface
	recorder    record.EventRecorder
	state       *StateManager
	cfg         Config
}

// Register initializes the Healer, starts state rehydration, hooks into namespace informers,
// and launches the background sync loop.
func Register(
	ctx context.Context,
	namespaces coreclient.NamespaceController,
	apiServices apiregistrationclient.APIServiceController,
	k8sClient kubernetes.Interface,
	recorder record.EventRecorder,
	cfg Config,
) (*Healer, error) {
	cfg.Complete()

	sm := NewStateManager(k8sClient, cfg.StateConfigMapNamespace, cfg.StateConfigMapName)
	if err := sm.Rehydrate(ctx); err != nil {
		logrus.Warnf("[%s] Initial state rehydration failed: %v", ControllerName, err)
	}

	h := &Healer{
		ctx:         ctx,
		namespaces:  namespaces,
		apiServices: apiServices,
		k8sClient:   k8sClient,
		recorder:    recorder,
		state:       sm,
		cfg:         cfg,
	}

	namespaces.OnChange(ctx, ControllerName, h.onChangeNamespace)

	go wait.UntilWithContext(ctx, h.sync, h.cfg.PollInterval)
	logrus.Infof("[%s] APIService healer controller registered (max detach duration: %v, poll interval: %v)", ControllerName, h.cfg.MaxDetachDuration, h.cfg.PollInterval)

	return h, nil
}

// onChangeNamespace is triggered on any namespace update, creation, or deletion event.
func (h *Healer) onChangeNamespace(key string, ns *corev1.Namespace) (*corev1.Namespace, error) {
	if ns == nil {
		// Namespace has been finalized and removed from cache
		h.cleanNamespaceFromWaiting(key)
		return nil, nil
	}

	// Only act on namespaces marked for deletion
	if ns.DeletionTimestamp == nil {
		h.cleanNamespaceFromWaiting(ns.Name)
		return nil, nil
	}

	// Check if this namespace has an active NamespaceDeletionDiscoveryFailure condition
	failingGroups := ExtractFailingGroupsFromNamespace(ns)
	if len(failingGroups) == 0 {
		h.cleanNamespaceFromWaiting(ns.Name)
		return nil, nil
	}

	// Safety Validation: Verify that ALL failing groups are strictly virtual non-storable
	safe, reason := ValidateSafeToDetach(failingGroups, h.cfg.NonStorableGroups)
	if !safe {
		logrus.Warnf("[%s] Namespace %s deletion blocked by discovery failure, but auto-remediation aborted: %s", ControllerName, ns.Name, reason)
		return nil, nil
	}

	// For each failing non-storable group, find and temporarily detach the failing APIService
	for _, group := range failingGroups {
		if err := h.remediateFailingGroup(ns, group); err != nil {
			logrus.Errorf("[%s] Failed to remediate failing group %s for namespace %s: %v", ControllerName, group, ns.Name, err)
		}
	}

	// Return nil, nil so Wrangler never attempts to update/write back to the Namespace resource.
	return nil, nil
}

// remediateFailingGroup identifies unavailable APIServices for the given group and detaches them.
func (h *Healer) remediateFailingGroup(ns *corev1.Namespace, group string) error {
	// List matching APIServices
	apiServices, err := h.getAPIServicesForGroup(group)
	if err != nil {
		return err
	}

	for _, svc := range apiServices {
		// Check if already detached
		if _, detached := h.state.GetDetached(svc.Name); detached {
			_ = h.state.AddWaitingNamespace(h.ctx, svc.Name, ns.Name)
			continue
		}

		// Verify that the APIService is actually in Available=False or Unknown condition
		if isAPIServiceAvailable(svc) {
			continue
		}

		logrus.Warnf("[%s] Detaching unavailable non-storable APIService %s to unblock namespace %s deletion", ControllerName, svc.Name, ns.Name)

		// Snapshot APIService state
		state := DetachedAPIServiceState{
			APIServiceName:    svc.Name,
			APIServiceSpec:    svc.Spec,
			Labels:            svc.Labels,
			Annotations:       svc.Annotations,
			DetachedAt:        time.Now(),
			WaitingNamespaces: []string{ns.Name},
		}

		if err := h.state.SaveDetached(h.ctx, state); err != nil {
			return fmt.Errorf("failed to persist detached state for %s: %w", svc.Name, err)
		}

		// Delete APIService from cluster
		if err := h.apiServices.Delete(svc.Name, &metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			_ = h.state.RemoveDetached(h.ctx, svc.Name)
			return fmt.Errorf("failed to delete APIService %s: %w", svc.Name, err)
		}

		if h.recorder != nil {
			h.recorder.Eventf(ns, corev1.EventTypeNormal, "APIServiceDetached", "Temporarily detached non-storable APIService %s to unblock deletion finalization", svc.Name)
		}
	}

	return nil
}

// sync runs periodically to check whether detached APIServices should be restored.
func (h *Healer) sync(ctx context.Context) {
	detachedList := h.state.ListDetached()
	if len(detachedList) == 0 {
		return
	}

	for _, detached := range detachedList {
		shouldRestore := false
		reason := ""

		// Condition 1: Max detach duration elapsed (safety timeout)
		if time.Since(detached.DetachedAt) > h.cfg.MaxDetachDuration {
			shouldRestore = true
			reason = fmt.Sprintf("max detach duration (%v) elapsed", h.cfg.MaxDetachDuration)
		} else {
			// Condition 2: All waiting namespaces have completed finalization
			remaining := h.filterActiveWaitingNamespaces(ctx, detached.WaitingNamespaces)
			if len(remaining) == 0 {
				shouldRestore = true
				reason = "all waiting namespaces completed finalization"
			} else if len(remaining) != len(detached.WaitingNamespaces) {
				// Update the reduced waiting list
				for _, ns := range detached.WaitingNamespaces {
					if !slices.Contains(remaining, ns) {
						_ = h.state.RemoveWaitingNamespace(ctx, detached.APIServiceName, ns)
					}
				}
			}
		}

		if shouldRestore {
			h.restoreAPIService(ctx, detached, reason)
		}
	}
}

// restoreAPIService recreates a previously detached APIService in the cluster.
func (h *Healer) restoreAPIService(ctx context.Context, detached DetachedAPIServiceState, reason string) {
	logrus.Infof("[%s] Restoring APIService %s (reason: %s)", ControllerName, detached.APIServiceName, reason)

	// Check if already recreated
	existing, err := h.apiServices.Get(detached.APIServiceName, metav1.GetOptions{})
	if err == nil && existing != nil {
		logrus.Infof("[%s] APIService %s already present in cluster", ControllerName, detached.APIServiceName)
		_ = h.state.RemoveDetached(ctx, detached.APIServiceName)
		return
	}

	// Recreate APIService
	restored := &apiregistrationv1.APIService{
		ObjectMeta: metav1.ObjectMeta{
			Name:        detached.APIServiceName,
			Labels:      detached.Labels,
			Annotations: detached.Annotations,
		},
		Spec: detached.APIServiceSpec,
	}

	if _, err := h.apiServices.Create(restored); err != nil && !apierrors.IsAlreadyExists(err) {
		logrus.Errorf("[%s] Failed to restore APIService %s: %v", ControllerName, detached.APIServiceName, err)
		return
	}

	logrus.Infof("[%s] Successfully restored APIService %s", ControllerName, detached.APIServiceName)
	_ = h.state.RemoveDetached(ctx, detached.APIServiceName)

	if h.recorder != nil && h.k8sClient != nil {
		ref := &corev1.ObjectReference{
			Kind:       "APIService",
			APIVersion: "apiregistration.k8s.io/v1",
			Name:       detached.APIServiceName,
		}
		h.recorder.Eventf(ref, corev1.EventTypeNormal, "APIServiceRestored", "Restored APIService %s: %s", detached.APIServiceName, reason)
	}
}

// filterActiveWaitingNamespaces returns only the namespaces from the list that still exist
// and still have a deletionTimestamp set.
func (h *Healer) filterActiveWaitingNamespaces(ctx context.Context, namespaces []string) []string {
	var active []string
	for _, name := range namespaces {
		ns, err := h.getNamespace(ctx, name)
		if err != nil || ns == nil {
			continue // Deleted
		}
		if ns.DeletionTimestamp != nil {
			active = append(active, name)
		}
	}
	return active
}

// cleanNamespaceFromWaiting removes a namespace from all detached APIServices' waiting lists.
func (h *Healer) cleanNamespaceFromWaiting(namespace string) {
	if namespace == "" {
		return
	}
	for _, detached := range h.state.ListDetached() {
		_ = h.state.RemoveWaitingNamespace(h.ctx, detached.APIServiceName, namespace)
	}
}

// getNamespace attempts cache lookup first, falling back to direct API client.
func (h *Healer) getNamespace(ctx context.Context, name string) (*corev1.Namespace, error) {
	if h.namespaces.Cache() != nil {
		ns, err := h.namespaces.Cache().Get(name)
		if err == nil {
			return ns, nil
		}
	}
	if h.k8sClient != nil {
		return h.k8sClient.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{})
	}
	return nil, apierrors.NewNotFound(corev1.Resource("namespaces"), name)
}

// getAPIServicesForGroup finds all APIServices that host the specified API group.
func (h *Healer) getAPIServicesForGroup(group string) ([]*apiregistrationv1.APIService, error) {
	group = strings.ToLower(strings.TrimSpace(group))
	var matches []*apiregistrationv1.APIService

	if h.apiServices.Cache() != nil {
		list, err := h.apiServices.Cache().List(labels.Everything())
		if err == nil {
			for _, svc := range list {
				if strings.ToLower(svc.Spec.Group) == group {
					matches = append(matches, svc)
				}
			}
			return matches, nil
		}
	}

	list, err := h.apiServices.List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for i := range list.Items {
		if strings.ToLower(list.Items[i].Spec.Group) == group {
			matches = append(matches, &list.Items[i])
		}
	}
	return matches, nil
}

// isAPIServiceAvailable checks if the APIService has condition Available == True.
func isAPIServiceAvailable(svc *apiregistrationv1.APIService) bool {
	for _, c := range svc.Status.Conditions {
		if c.Type == apiregistrationv1.Available {
			return c.Status == apiregistrationv1.ConditionTrue
		}
	}
	return false
}
