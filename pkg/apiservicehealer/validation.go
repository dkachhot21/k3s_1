package apiservicehealer

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// groupVersionRegex matches API group/version path patterns, including:
// 1. Domain-style groups (e.g. metrics.k8s.io/v1beta1, wardle.k8s.io/v1alpha1, cert-manager.io/v1)
// 2. Short/core groups without dots (e.g. apps/v1, batch/v1)
var groupVersionRegex = regexp.MustCompile(`\b([a-zA-Z0-9][-a-zA-Z0-9_.]*)/([vV][a-zA-Z0-9]+[a-zA-Z0-9._-]*)\b`)

// apiServiceNameRegex matches APIService object name patterns (<version>.<group>),
// e.g. v1beta1.metrics.k8s.io, v1alpha1.wardle.k8s.io, v1.cert-manager.io
var apiServiceNameRegex = regexp.MustCompile(`\b([vV][a-zA-Z0-9]+)\.([a-zA-Z0-9][-a-zA-Z0-9_.]+\.[a-zA-Z]{2,})\b`)

// IsNonStorableGroup checks whether an API group is permitted to be bypassed.
// It checks default virtual groups, custom configured groups, and the KUBE_NON_STORABLE_API_GROUPS env var.
// By default, ONLY purely virtual telemetry APIs (metrics.k8s.io, custom.metrics.k8s.io, external.metrics.k8s.io)
// are allowed. All other API groups (core APIs, CRDs, and stateful aggregated APIServices) return false.
func IsNonStorableGroup(group string, customGroups []string) bool {
	group = strings.ToLower(strings.TrimSpace(group))
	if group == "" {
		return false
	}

	for _, g := range DefaultNonStorableAPIGroups {
		if strings.ToLower(strings.TrimSpace(g)) == group {
			return true
		}
	}

	for _, g := range customGroups {
		if strings.ToLower(strings.TrimSpace(g)) == group {
			return true
		}
	}

	if env := os.Getenv("KUBE_NON_STORABLE_API_GROUPS"); env != "" {
		for _, g := range strings.Split(env, ",") {
			if strings.ToLower(strings.TrimSpace(g)) == group {
				return true
			}
		}
	}

	if env := os.Getenv("NON_STORABLE_API_GROUPS"); env != "" {
		for _, g := range strings.Split(env, ",") {
			if strings.ToLower(strings.TrimSpace(g)) == group {
				return true
			}
		}
	}

	return false
}

// ExtractFailingGroupsFromNamespace parses the NamespaceDeletionDiscoveryFailure condition
// of a namespace to identify which API groups caused the discovery failure.
func ExtractFailingGroupsFromNamespace(ns *corev1.Namespace) []string {
	if ns == nil {
		return nil
	}

	var rawMessage string
	for _, c := range ns.Status.Conditions {
		if c.Type == corev1.NamespaceDeletionDiscoveryFailure && c.Status == corev1.ConditionTrue {
			rawMessage = c.Message
			break
		}
	}

	if rawMessage == "" {
		return nil
	}

	return ExtractGroupsFromMessage(rawMessage)
}

// ExtractGroupsFromMessage extracts unique API group names from a discovery error message string.
// It handles:
// - Group/version path format (e.g. "metrics.k8s.io/v1beta1", "apps/v1", "wardle.k8s.io/v1alpha1")
// - APIService name format (e.g. "v1beta1.metrics.k8s.io", "v1alpha1.wardle.k8s.io")
// - Direct API group names
func ExtractGroupsFromMessage(msg string) []string {
	seen := make(map[string]bool)
	var groups []string

	// Match group/version path patterns (e.g. metrics.k8s.io/v1beta1, apps/v1, wardle.k8s.io/v1alpha1)
	for _, match := range groupVersionRegex.FindAllStringSubmatch(msg, -1) {
		if len(match) > 1 {
			group := strings.ToLower(strings.TrimSpace(match[1]))
			if group != "" && !seen[group] {
				seen[group] = true
				groups = append(groups, group)
			}
		}
	}

	// Match APIService name patterns (e.g. v1beta1.metrics.k8s.io, v1alpha1.wardle.k8s.io)
	for _, match := range apiServiceNameRegex.FindAllStringSubmatch(msg, -1) {
		if len(match) > 2 {
			group := strings.ToLower(strings.TrimSpace(match[2]))
			if group != "" && !seen[group] {
				seen[group] = true
				groups = append(groups, group)
			}
		}
	}

	// Check known default groups directly in message in case version was omitted
	for _, defaultGroup := range DefaultNonStorableAPIGroups {
		lowered := strings.ToLower(defaultGroup)
		if strings.Contains(strings.ToLower(msg), lowered) && !seen[lowered] {
			seen[lowered] = true
			groups = append(groups, lowered)
		}
	}

	return groups
}

// ValidateSafeToDetach enforces that EVERY failing API group blocking namespace deletion
// belongs strictly to the permitted non-storable virtual API list.
// If ANY failing group is stateful/persistent (core APIs, CRDs, or stateful aggregated APIServices),
// detachment is rejected to protect persistent etcd state from being orphaned.
func ValidateSafeToDetach(failingGroups []string, customGroups []string) (bool, string) {
	if len(failingGroups) == 0 {
		return false, "no failing API groups detected in namespace discovery failure"
	}

	for _, group := range failingGroups {
		if !IsNonStorableGroup(group, customGroups) {
			return false, fmt.Sprintf("group %q is not in the permitted non-storable virtual API list; auto-remediation aborted to protect persistent data", group)
		}
	}

	return true, ""
}
