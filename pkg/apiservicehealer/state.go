package apiservicehealer

import (
	"context"
	"encoding/json"
	"slices"
	"sync"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// StateManager provides thread-safe access to detached APIService state
// backed by both an in-memory cache and a Kubernetes ConfigMap for crash resilience.
type StateManager struct {
	mu        sync.RWMutex
	k8sClient kubernetes.Interface
	namespace string
	name      string
	detached  map[string]DetachedAPIServiceState
}

// NewStateManager creates and returns an initialized StateManager.
func NewStateManager(k8sClient kubernetes.Interface, namespace, name string) *StateManager {
	if namespace == "" {
		namespace = DefaultStateConfigMapNamespace
	}
	if name == "" {
		name = DefaultStateConfigMapName
	}
	return &StateManager{
		k8sClient: k8sClient,
		namespace: namespace,
		name:      name,
		detached:  make(map[string]DetachedAPIServiceState),
	}
}

// Rehydrate loads persistent state from the ConfigMap into the local in-memory store.
func (sm *StateManager) Rehydrate(ctx context.Context) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.k8sClient == nil {
		return nil
	}

	cm, err := sm.k8sClient.CoreV1().ConfigMaps(sm.namespace).Get(ctx, sm.name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			logrus.Debugf("[%s] State ConfigMap %s/%s not found; starting with empty state", ControllerName, sm.namespace, sm.name)
			return nil
		}
		return err
	}

	raw, ok := cm.Data[StateDataKey]
	if !ok || raw == "" {
		return nil
	}

	var ps PersistedState
	if err := json.Unmarshal([]byte(raw), &ps); err != nil {
		logrus.Warnf("[%s] Failed to unmarshal state from ConfigMap: %v", ControllerName, err)
		return err
	}

	if ps.Detached != nil {
		sm.detached = ps.Detached
		logrus.Infof("[%s] Rehydrated %d detached APIService state entries from %s/%s", ControllerName, len(sm.detached), sm.namespace, sm.name)
	}

	return nil
}

// SaveDetached records a detached APIService state both in memory and in the ConfigMap.
func (sm *StateManager) SaveDetached(ctx context.Context, state DetachedAPIServiceState) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.detached[state.APIServiceName] = state
	return sm.persist(ctx)
}

// RemoveDetached removes an APIService from tracking after it has been restored.
func (sm *StateManager) RemoveDetached(ctx context.Context, apiServiceName string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, ok := sm.detached[apiServiceName]; !ok {
		return nil
	}

	delete(sm.detached, apiServiceName)
	return sm.persist(ctx)
}

// GetDetached retrieves a detached APIService state by name from the in-memory cache.
func (sm *StateManager) GetDetached(apiServiceName string) (DetachedAPIServiceState, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	state, ok := sm.detached[apiServiceName]
	return state, ok
}

// ListDetached returns a snapshot slice of all currently detached APIServices.
func (sm *StateManager) ListDetached() []DetachedAPIServiceState {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	list := make([]DetachedAPIServiceState, 0, len(sm.detached))
	for _, s := range sm.detached {
		list = append(list, s)
	}
	return list
}

// AddWaitingNamespace registers a namespace as waiting for a detached APIService to restore.
func (sm *StateManager) AddWaitingNamespace(ctx context.Context, apiServiceName, namespace string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	state, ok := sm.detached[apiServiceName]
	if !ok {
		return nil
	}

	if !slices.Contains(state.WaitingNamespaces, namespace) {
		state.WaitingNamespaces = append(state.WaitingNamespaces, namespace)
		sm.detached[apiServiceName] = state
		return sm.persist(ctx)
	}

	return nil
}

// RemoveWaitingNamespace removes a namespace from the waiting list of a detached APIService.
func (sm *StateManager) RemoveWaitingNamespace(ctx context.Context, apiServiceName, namespace string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	state, ok := sm.detached[apiServiceName]
	if !ok {
		return nil
	}

	idx := slices.Index(state.WaitingNamespaces, namespace)
	if idx >= 0 {
		state.WaitingNamespaces = slices.Delete(state.WaitingNamespaces, idx, idx+1)
		sm.detached[apiServiceName] = state
		return sm.persist(ctx)
	}

	return nil
}

// persist synchronizes the in-memory detached state map to the Kubernetes ConfigMap.
// Must be called with sm.mu held.
func (sm *StateManager) persist(ctx context.Context) error {
	if sm.k8sClient == nil {
		return nil
	}

	data, err := json.Marshal(PersistedState{Detached: sm.detached})
	if err != nil {
		return err
	}

	cm, err := sm.k8sClient.CoreV1().ConfigMaps(sm.namespace).Get(ctx, sm.name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			newCM := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: sm.namespace,
					Name:      sm.name,
					Labels: map[string]string{
						"app.kubernetes.io/name":       ControllerName,
						"app.kubernetes.io/managed-by": "k3s",
					},
				},
				Data: map[string]string{
					StateDataKey: string(data),
				},
			}
			_, err = sm.k8sClient.CoreV1().ConfigMaps(sm.namespace).Create(ctx, newCM, metav1.CreateOptions{})
			return err
		}
		return err
	}

	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}
	cm.Data[StateDataKey] = string(data)
	_, err = sm.k8sClient.CoreV1().ConfigMaps(sm.namespace).Update(ctx, cm, metav1.UpdateOptions{})
	return err
}
