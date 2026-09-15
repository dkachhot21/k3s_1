package apiservicehealer

import (
	"time"

	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
)

const (
	// DefaultMaxDetachDuration is the default timeout after which a detached APIService
	// is restored even if waiting namespaces are still terminating.
	DefaultMaxDetachDuration = 5 * time.Minute

	// DefaultPollInterval is the default period between reconciliation sync passes.
	DefaultPollInterval = 10 * time.Second

	// DefaultStateConfigMapNamespace is the namespace where healer state is persisted.
	DefaultStateConfigMapNamespace = "kube-system"

	// DefaultStateConfigMapName is the ConfigMap name storing persistent detached state.
	DefaultStateConfigMapName = "apiservice-healer-state"

	// StateDataKey is the key inside the ConfigMap data map containing the JSON state.
	StateDataKey = "state.json"

	// ControllerName is the human-readable identifier for this reconciler.
	ControllerName = "apiservice-healer"
)

// DefaultNonStorableAPIGroups lists known purely virtual, in-memory telemetry API groups.
// These APIs never store persistent state in etcd and can safely be detached during outages.
var DefaultNonStorableAPIGroups = []string{
	"metrics.k8s.io",
	"custom.metrics.k8s.io",
	"external.metrics.k8s.io",
}

// Config defines the runtime configuration options for the APIService healer reconciler.
type Config struct {
	// NonStorableGroups contains additional API groups permitted to be detached during deletion.
	NonStorableGroups []string

	// MaxDetachDuration specifies the maximum time an APIService may remain detached
	// before automatic restoration is forced.
	MaxDetachDuration time.Duration

	// PollInterval defines the period between periodic sweeps.
	PollInterval time.Duration

	// StateConfigMapNamespace is the namespace for the persistence ConfigMap.
	StateConfigMapNamespace string

	// StateConfigMapName is the ConfigMap name for persistence.
	StateConfigMapName string
}

// Complete sets default values for any unspecified configuration fields.
func (c *Config) Complete() {
	if c.MaxDetachDuration <= 0 {
		c.MaxDetachDuration = DefaultMaxDetachDuration
	}
	if c.PollInterval <= 0 {
		c.PollInterval = DefaultPollInterval
	}
	if c.StateConfigMapNamespace == "" {
		c.StateConfigMapNamespace = DefaultStateConfigMapNamespace
	}
	if c.StateConfigMapName == "" {
		c.StateConfigMapName = DefaultStateConfigMapName
	}
}

// DetachedAPIServiceState records metadata and specifications for a detached APIService.
type DetachedAPIServiceState struct {
	APIServiceName    string                               `json:"apiServiceName"`
	APIServiceSpec    apiregistrationv1.APIServiceSpec     `json:"apiServiceSpec"`
	Labels            map[string]string                    `json:"labels,omitempty"`
	Annotations       map[string]string                    `json:"annotations,omitempty"`
	DetachedAt        time.Time                            `json:"detachedAt"`
	WaitingNamespaces []string                             `json:"waitingNamespaces"`
}

// PersistedState represents the serialized structure stored in the state ConfigMap.
type PersistedState struct {
	Detached map[string]DetachedAPIServiceState `json:"detached"`
}
