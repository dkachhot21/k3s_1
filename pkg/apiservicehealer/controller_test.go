package apiservicehealer

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
)

func Test_UnitIsNonStorableGroup(t *testing.T) {
	tests := []struct {
		group        string
		custom       []string
		wantResult   bool
	}{
		{"metrics.k8s.io", nil, true},
		{"custom.metrics.k8s.io", nil, true},
		{"external.metrics.k8s.io", nil, true},
		{"METRICS.K8S.IO", nil, true},
		{"apps", nil, false},
		{"batch", nil, false},
		{"mycrd.example.com", nil, false},
		{"mycrd.example.com", []string{"mycrd.example.com"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.group, func(t *testing.T) {
			got := IsNonStorableGroup(tt.group, tt.custom)
			if got != tt.wantResult {
				t.Fatalf("IsNonStorableGroup(%q) = %v; want %v", tt.group, got, tt.wantResult)
			}
		})
	}
}

func Test_UnitExtractGroupsFromMessage(t *testing.T) {
	msg := "Discovery failed for some groups, 1 failing: unable to retrieve the complete list of server APIs: metrics.k8s.io/v1beta1: the server is currently unable to handle the request"
	groups := ExtractGroupsFromMessage(msg)
	if len(groups) != 1 || groups[0] != "metrics.k8s.io" {
		t.Fatalf("unexpected extracted groups: %v", groups)
	}

	mixedMsg := "2 failing: metrics.k8s.io/v1beta1: timeout, custom.storage.io/v1: not found"
	mixedGroups := ExtractGroupsFromMessage(mixedMsg)
	if len(mixedGroups) != 2 {
		t.Fatalf("expected 2 groups, got %v", mixedGroups)
	}
}

func Test_UnitValidateSafeToDetach(t *testing.T) {
	// Virtual non-storable only -> safe
	safe, reason := ValidateSafeToDetach([]string{"metrics.k8s.io"}, nil)
	if !safe {
		t.Fatalf("expected safe, got reason: %s", reason)
	}

	// Stateful only -> abort
	safe, reason = ValidateSafeToDetach([]string{"apps"}, nil)
	if safe {
		t.Fatalf("expected unsafe for stateful group 'apps'")
	}

	// Mixed (metrics + stateful) -> abort
	safe, reason = ValidateSafeToDetach([]string{"metrics.k8s.io", "custom.database.io"}, nil)
	if safe {
		t.Fatalf("expected unsafe for mixed groups")
	}

	// Empty -> abort
	safe, _ = ValidateSafeToDetach(nil, nil)
	if safe {
		t.Fatalf("expected unsafe for empty groups")
	}
}

func Test_UnitStateManager(t *testing.T) {
	ctx := context.Background()
	fakeClient := fake.NewClientset()
	sm := NewStateManager(fakeClient, "kube-system", "test-healer-state")

	// Initial save
	state := DetachedAPIServiceState{
		APIServiceName: "v1beta1.metrics.k8s.io",
		APIServiceSpec: apiregistrationv1.APIServiceSpec{
			Group:   "metrics.k8s.io",
			Version: "v1beta1",
		},
		DetachedAt:        time.Now(),
		WaitingNamespaces: []string{"test-ns-1"},
	}

	if err := sm.SaveDetached(ctx, state); err != nil {
		t.Fatalf("SaveDetached failed: %v", err)
	}

	// Read from cache
	cached, ok := sm.GetDetached("v1beta1.metrics.k8s.io")
	if !ok || cached.APIServiceName != "v1beta1.metrics.k8s.io" {
		t.Fatalf("GetDetached failed to retrieve saved state")
	}

	// Add waiting namespace
	if err := sm.AddWaitingNamespace(ctx, "v1beta1.metrics.k8s.io", "test-ns-2"); err != nil {
		t.Fatalf("AddWaitingNamespace failed: %v", err)
	}
	cached, _ = sm.GetDetached("v1beta1.metrics.k8s.io")
	if len(cached.WaitingNamespaces) != 2 {
		t.Fatalf("expected 2 waiting namespaces, got %v", cached.WaitingNamespaces)
	}

	// Test Rehydration into a new StateManager instance
	sm2 := NewStateManager(fakeClient, "kube-system", "test-healer-state")
	if err := sm2.Rehydrate(ctx); err != nil {
		t.Fatalf("Rehydrate failed: %v", err)
	}
	cached2, ok := sm2.GetDetached("v1beta1.metrics.k8s.io")
	if !ok || len(cached2.WaitingNamespaces) != 2 {
		t.Fatalf("Rehydration failed to restore state from ConfigMap")
	}

	// Remove waiting namespace
	if err := sm.RemoveWaitingNamespace(ctx, "v1beta1.metrics.k8s.io", "test-ns-1"); err != nil {
		t.Fatalf("RemoveWaitingNamespace failed: %v", err)
	}
	cached, _ = sm.GetDetached("v1beta1.metrics.k8s.io")
	if len(cached.WaitingNamespaces) != 1 || cached.WaitingNamespaces[0] != "test-ns-2" {
		t.Fatalf("unexpected waiting namespaces after removal: %v", cached.WaitingNamespaces)
	}

	// Remove detached
	if err := sm.RemoveDetached(ctx, "v1beta1.metrics.k8s.io"); err != nil {
		t.Fatalf("RemoveDetached failed: %v", err)
	}
	if _, ok := sm.GetDetached("v1beta1.metrics.k8s.io"); ok {
		t.Fatalf("expected state to be removed")
	}
}

func Test_UnitExtractFailingGroupsFromNamespace(t *testing.T) {
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "stuck-ns"},
		Status: corev1.NamespaceStatus{
			Conditions: []corev1.NamespaceCondition{
				{
					Type:    corev1.NamespaceDeletionDiscoveryFailure,
					Status:  corev1.ConditionTrue,
					Message: "Discovery failed for some groups, 1 failing: unable to retrieve the complete list of server APIs: metrics.k8s.io/v1beta1: service unavailable",
				},
			},
		},
	}

	groups := ExtractFailingGroupsFromNamespace(ns)
	if len(groups) != 1 || groups[0] != "metrics.k8s.io" {
		t.Fatalf("expected ['metrics.k8s.io'], got %v", groups)
	}
}
