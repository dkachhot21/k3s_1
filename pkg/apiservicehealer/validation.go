package apiservicehealer

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// groupRegex matches API group patterns such as metrics.k8s.io, custom.metrics.k8s.io, etc.
var groupVersionRegex = regexp.MustCompile(`([a-zA-Z0-9][-a-zA-Z0-9_.]+\.[a-zA-Z]{2,})/([vV][a-zA-Z0-9]+)`)

// IsNonStorableGroup checks whether an API group is permitted to be bypassed.
// It checks default virtual groups, custom configured groups, and the KUBE_NON_STORABLE_API_GROUPS env var.
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
func ExtractGroupsFromMessage(msg string) []string {
	seen := make(map[string]bool)
	var groups []string

	// First match group/version patterns (e.g. metrics.k8s.io/v1beta1)
	matches := groupVersionRegex.FindAllStringSubmatch(msg, -1)
	for _, match := range matches {
		if len(match) > 1 {
			group := strings.ToLower(strings.TrimSpace(match[1]))
			if group != "" && !seen[group] {
				seen[group] = true
				groups = append(groups, group)
			}
		}
	}

	// Also check known default groups directly in message in case version was omitted
	for _, defaultGroup := range DefaultNonStorableAPIGroups {
		lowered := strings.ToLower(defaultGroup)
		if strings.Contains(strings.ToLower(msg), lowered) && !seen[lowered] {
			seen[lowered] = true
			groups = append(groups, lowered)
		}
	}

	return groups
}

// ValidateSafeToDetach verifies that EVERY failing API group blocking the namespace
// belongs to the permitted non-storable virtual API list.
// If any failing group is stateful (e.g. CRD, apps, core), detachment is rejected
// to prevent orphaning persistent data in etcd.
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
