package apiservicehealer

import (
	"context"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
)

func Test_UnitIsNonStorableGroup(t *testing.T) {
	tests := []struct {
		name       string
		group      string
		custom     []string
		wantResult bool
	}{
		{"default virtual metrics", "metrics.k8s.io", nil, true},
		{"default virtual custom metrics", "custom.metrics.k8s.io", nil, true},
		{"default virtual external metrics", "external.metrics.k8s.io", nil, true},
		{"case insensitive metrics", "METRICS.K8S.IO", nil, true},
		{"core stateful apps", "apps", nil, false},
		{"core stateful batch", "batch", nil, false},
		{"stateful CRD cert-manager", "cert-manager.io", nil, false},
		{"stateful aggregated API wardle", "wardle.k8s.io", nil, false},
		{"custom virtual group via argument", "myvirtual.telemetry.io", []string{"myvirtual.telemetry.io"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsNonStorableGroup(tt.group, tt.custom)
			if got != tt.wantResult {
				t.Fatalf("IsNonStorableGroup(%q) = %v; want %v", tt.group, got, tt.wantResult)
			}
		})
	}
}

func Test_UnitIsNonStorableGroup_EnvVar(t *testing.T) {
	os.Setenv("NON_STORABLE_API_GROUPS", "dyn.virtual.io,dyn2.virtual.io")
	defer os.Unsetenv("NON_STORABLE_API_GROUPS")

	if !IsNonStorableGroup("dyn.virtual.io", nil) {
		t.Fatalf("expected dyn.virtual.io to be recognized from NON_STORABLE_API_GROUPS")
	}
	if IsNonStorableGroup("stateful.db.io", nil) {
		t.Fatalf("expected stateful.db.io to be rejected")
	}
}

func Test_UnitExtractGroupsFromMessage(t *testing.T) {
	// Standard Group/Version format
	msg1 := "Discovery failed for some groups, 1 failing: unable to retrieve the complete list of server APIs: metrics.k8s.io/v1beta1: the server is currently unable to handle the request"
	g1 := ExtractGroupsFromMessage(msg1)
	if len(g1) != 1 || g1[0] != "metrics.k8s.io" {
		t.Fatalf("msg1: expected ['metrics.k8s.io'], got %v", g1)
	}

	// Core API format (apps/v1) without dots in group
	msgCore := "Discovery failed for some groups, 1 failing: unable to retrieve the complete list of server APIs: apps/v1: connection refused"
	gCore := ExtractGroupsFromMessage(msgCore)
	if len(gCore) != 1 || gCore[0] != "apps" {
		t.Fatalf("msgCore: expected ['apps'], got %v", gCore)
	}

	// Stateful aggregated APIService (wardle.k8s.io/v1alpha1)
	msgStatefulAggregated := "unable to retrieve the complete list of server APIs: wardle.k8s.io/v1alpha1: connection timeout"
	gWardle := ExtractGroupsFromMessage(msgStatefulAggregated)
	if len(gWardle) != 1 || gWardle[0] != "wardle.k8s.io" {
		t.Fatalf("msgWardle: expected ['wardle.k8s.io'], got %v", gWardle)
	}

	// APIService name format: v1beta1.metrics.k8s.io
	msgAPISvc := "APIService v1beta1.metrics.k8s.io: failing or stopped service"
	gAPISvc := ExtractGroupsFromMessage(msgAPISvc)
	if len(gAPISvc) != 1 || gAPISvc[0] != "metrics.k8s.io" {
		t.Fatalf("msgAPISvc: expected ['metrics.k8s.io'], got %v", gAPISvc)
	}

	// Mixed: metrics.k8s.io + stateful CRD cert-manager.io
	mixedMsg := "2 failing: metrics.k8s.io/v1beta1: timeout, cert-manager.io/v1: not found"
	mixedGroups := ExtractGroupsFromMessage(mixedMsg)
	if len(mixedGroups) != 2 {
		t.Fatalf("expected 2 groups, got %v", mixedGroups)
	}
}

func Test_UnitValidateSafeToDetach_StatefulAPIServices(t *testing.T) {
	// Virtual non-storable only -> MUST pass
	safe, reason := ValidateSafeToDetach([]string{"metrics.k8s.io"}, nil)
	if !safe {
		t.Fatalf("expected safe for metrics.k8s.io, got reason: %s", reason)
	}

	// Stateful aggregated APIService (wardle.k8s.io) -> MUST abort
	safe, reason = ValidateSafeToDetach([]string{"wardle.k8s.io"}, nil)
	if safe {
		t.Fatalf("expected unsafe for stateful aggregated APIService wardle.k8s.io")
	}
	if reason == "" {
		t.Fatalf("expected explanation why wardle.k8s.io is aborted")
	}

	// Stateful CRD (cert-manager.io) -> MUST abort
	safe, _ = ValidateSafeToDetach([]string{"cert-manager.io"}, nil)
	if safe {
		t.Fatalf("expected unsafe for stateful CRD cert-manager.io")
	}

	// Core API (apps) -> MUST abort
	safe, _ = ValidateSafeToDetach([]string{"apps"}, nil)
	if safe {
		t.Fatalf("expected unsafe for core group apps")
	}

	// Mixed: virtual (metrics.k8s.io) + stateful (wardle.k8s.io) -> MUST abort
	safe, _ = ValidateSafeToDetach([]string{"metrics.k8s.io", "wardle.k8s.io"}, nil)
	if safe {
		t.Fatalf("expected unsafe for mixed virtual and stateful groups")
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
