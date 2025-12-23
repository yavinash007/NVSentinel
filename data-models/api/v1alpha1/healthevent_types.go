package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:validation:Enum=NotStarted;InProgress;Failed;Succeeded;AlreadyDrained;\
// UnQuarantined;Quarantined;AlreadyQuarantined;Cancelled
type Status string

const (
	StatusNotStarted     Status = "NotStarted"
	StatusInProgress     Status = "InProgress"
	StatusFailed         Status = "Failed"
	StatusSucceeded      Status = "Succeeded"
	StatusAlreadyDrained Status = "AlreadyDrained"

	StatusUnQuarantined      Status = "UnQuarantined"
	StatusQuarantined        Status = "Quarantined"
	StatusAlreadyQuarantined Status = "AlreadyQuarantined"
	StatusCancelled          Status = "Cancelled"
)

// +kubebuilder:validation:Enum=NONE;COMPONENT_RESET;CONTACT_SUPPORT;\
// RUN_FIELDDIAG;RESTART_VM;RESTART_BM;REPLACE_VM;RUN_DCGMEUD;UNKNOWN
type RecommendedAction string

const (
	RecommendedActionNone           RecommendedAction = "NONE"
	RecommendedActionComponentReset RecommendedAction = "COMPONENT_RESET"
	RecommendedActionContactSupport RecommendedAction = "CONTACT_SUPPORT"
	RecommendedActionRunFieldDiag   RecommendedAction = "RUN_FIELDDIAG"
	RecommendedActionRestartVM      RecommendedAction = "RESTART_VM"
	RecommendedActionRestartBM      RecommendedAction = "RESTART_BM"
	RecommendedActionReplaceVM      RecommendedAction = "REPLACE_VM"
	RecommendedActionRunDCGMEUD     RecommendedAction = "RUN_DCGMEUD"
	RecommendedActionUnknown        RecommendedAction = "UNKNOWN"
)

// +kubebuilder:validation:Enum=UNSPECIFIED;EXECUTE_REMEDIATION;STORE_ONLY
type ProcessingStrategy string

const (
	ProcessingStrategyUnspecified        ProcessingStrategy = "UNSPECIFIED"
	ProcessingStrategyExecuteRemediation ProcessingStrategy = "EXECUTE_REMEDIATION"
	ProcessingStrategyStoreOnly          ProcessingStrategy = "STORE_ONLY"
)

// OperationStatus represents the status of a sub-operation
type OperationStatus struct {
	// +kubebuilder:validation:Required
	Status Status `json:"status,omitempty"`

	Message string `json:"message,omitempty"`
}

// HealthEvent Status
// HealthEventStatus defines the observed state of HealthEvent
type HealthEventStatus struct {
	// Whether the node has been quarantined
	NodeQuarantined *Status `json:"nodeQuarantined,omitempty"`

	// Status of user pod eviction
	UserPodsEvictionStatus OperationStatus `json:"userPodsEvictionStatus"`

	// Whether the fault has been remediated
	FaultRemediated *bool `json:"faultRemediated,omitempty"`

	// Timestamp of the last remediation attempt
	LastRemediationTimestamp *metav1.Time `json:"lastRemediationTimestamp,omitempty"`
}

// HealthEvent Spec
// BehaviourOverrides controls whether specific remediation steps should be
// forced or skipped during the remediation workflow.
type BehaviourOverrides struct {
	// Force indicates that the remediation step should be executed even if
	// normal conditions would prevent it.
	Force bool `json:"force,omitempty"`

	// Skip indicates that the remediation step should be skipped entirely.
	Skip bool `json:"skip,omitempty"`
}

// Entity represents a target or affected object referenced by a HealthEvent.
// Examples include a node, pod, or component.
type Entity struct {
	// EntityType is the type of entity (e.g., "node", "pod", "component").
	EntityType string `json:"entityType,omitempty"`

	// EntityValue is the identifier or name of the entity.
	// For example: node name, pod name, or component ID.
	EntityValue string `json:"entityValue,omitempty"`
}

type HealthEventSpec struct {
	// Version of the event
	Version uint32 `json:"version,omitempty"`

	// Agent that generated the event
	Agent string `json:"agent,omitempty"`

	// Component class
	ComponentClass string `json:"componentClass,omitempty"`

	// Name of the check
	CheckName string `json:"checkName,omitempty"`

	// Whether the fault is fatal
	IsFatal *bool `json:"isFatal,omitempty"`

	// Whether the system is healthy
	IsHealthy *bool `json:"isHealthy,omitempty"`

	// Event message
	Message string `json:"message,omitempty"`

	// Recommended remediation action
	RecommendedAction RecommendedAction `json:"recommendedAction,omitempty"`

	// Error codes
	ErrorCode []string `json:"errorCode,omitempty"`

	// Entities impacted
	EntitiesImpacted []*Entity `json:"entitiesImpacted,omitempty"`

	// Metadata map
	Metadata map[string]string `json:"metadata,omitempty"`

	// Timestamp when the event was generated
	GeneratedTimestamp *metav1.Time `json:"generatedTimestamp,omitempty"`

	// NodeName is the name of the target node
	NodeName string `json:"nodeName,omitempty"`

	// Behaviour overrides for quarantine
	QuarantineOverrides *BehaviourOverrides `json:"quarantineOverrides,omitempty"`

	// Behaviour overrides for drain
	DrainOverrides *BehaviourOverrides `json:"drainOverrides,omitempty"`

	// ProcessingStrategy
	ProcessingStrategy ProcessingStrategy `json:"processingStrategy,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type HealthEvent struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HealthEventSpec   `json:"spec,omitempty"`
	Status HealthEventStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type HealthEventList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HealthEvent `json:"items"`
}

func init() {
	SchemeBuilder.Register(&HealthEvent{}, &HealthEventList{})
}
