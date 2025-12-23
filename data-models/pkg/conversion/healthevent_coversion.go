package conversion

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/nvidia/nvsentinel/data-models/api/v1alpha1"
	"github.com/nvidia/nvsentinel/data-models/pkg/model"
	"github.com/nvidia/nvsentinel/data-models/pkg/protos"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// NOTE: These assume model.Status and v1alpha1.Status
// have identical underlying values.
func ModelStatusToCRD(s model.Status) v1alpha1.Status {
	return v1alpha1.Status(s)
}

func CRDStatusToModel(s v1alpha1.Status) model.Status {
	return model.Status(s)
}

// OperationStatus conversions
func ModelOpStatusToCRD(m model.OperationStatus) v1alpha1.OperationStatus {
	return v1alpha1.OperationStatus{
		Status:  ModelStatusToCRD(m.Status),
		Message: m.Message,
	}
}

func CRDOpStatusToModel(c v1alpha1.OperationStatus) model.OperationStatus {
	return model.OperationStatus{
		Status:  CRDStatusToModel(c.Status),
		Message: c.Message,
	}
}

// HealthEventStatus conversions
func ModelHealthEventStatusToCRD(m *model.HealthEventStatus) *v1alpha1.HealthEventStatus {
	if m == nil {
		return nil
	}

	out := &v1alpha1.HealthEventStatus{
		NodeQuarantined: func() *v1alpha1.Status {
			if m.NodeQuarantined == nil {
				return nil
			}

			s := ModelStatusToCRD(*m.NodeQuarantined)

			return &s
		}(),
		UserPodsEvictionStatus: ModelOpStatusToCRD(m.UserPodsEvictionStatus),
		FaultRemediated:        m.FaultRemediated,
	}

	if m.LastRemediationTimestamp != nil {
		t := metav1.NewTime(*m.LastRemediationTimestamp)
		out.LastRemediationTimestamp = &t
	}

	return out
}

func CRDHealthEventStatusToModel(c *v1alpha1.HealthEventStatus) *model.HealthEventStatus {
	if c == nil {
		return nil
	}

	out := &model.HealthEventStatus{
		NodeQuarantined: func() *model.Status {
			if c.NodeQuarantined == nil {
				return nil
			}

			s := CRDStatusToModel(*c.NodeQuarantined)

			return &s
		}(),
		UserPodsEvictionStatus: CRDOpStatusToModel(c.UserPodsEvictionStatus),
		FaultRemediated:        c.FaultRemediated,
	}

	if c.LastRemediationTimestamp != nil {
		t := c.LastRemediationTimestamp.Time
		out.LastRemediationTimestamp = &t
	}

	return out
}

// BehaviourOverrides conversions
func ProtoBehaviourOverridesToCRD(p *protos.BehaviourOverrides) *v1alpha1.BehaviourOverrides {
	if p == nil {
		return nil
	}

	return &v1alpha1.BehaviourOverrides{
		Force: p.Force,
		Skip:  p.Skip,
	}
}

func CRDBehaviourOverridesToProto(c *v1alpha1.BehaviourOverrides) *protos.BehaviourOverrides {
	if c == nil {
		return nil
	}

	return &protos.BehaviourOverrides{
		Force: c.Force,
		Skip:  c.Skip,
	}
}

// Entity conversions
func ProtoEntityToCRD(p *protos.Entity) *v1alpha1.Entity {
	if p == nil {
		return nil
	}

	return &v1alpha1.Entity{
		EntityType:  p.EntityType,
		EntityValue: p.EntityValue,
	}
}

func CRDEntityToProto(c *v1alpha1.Entity) *protos.Entity {
	if c == nil {
		return nil
	}

	return &protos.Entity{
		EntityType:  c.EntityType,
		EntityValue: c.EntityValue,
	}
}

// ProtoHealthEventToCRDSpec converts a protobuf HealthEvent into a CRD HealthEventSpec.
// It maps each field from the protobuf model to the Kubernetes CRD representation.
func ProtoHealthEventToCRDSpec(p *protos.HealthEvent) v1alpha1.HealthEventSpec {
	if p == nil {
		return v1alpha1.HealthEventSpec{}
	}

	spec := v1alpha1.HealthEventSpec{
		Version:             p.Version,
		Agent:               p.Agent,
		ComponentClass:      p.ComponentClass,
		CheckName:           p.CheckName,
		IsFatal:             &p.IsFatal,
		IsHealthy:           &p.IsHealthy,
		Message:             p.Message,
		RecommendedAction:   ProtoRecommendedActionToCRD(p.RecommendedAction),
		ErrorCode:           p.ErrorCode,
		Metadata:            p.Metadata,
		NodeName:            p.NodeName,
		QuarantineOverrides: ProtoBehaviourOverridesToCRD(p.QuarantineOverrides),
		DrainOverrides:      ProtoBehaviourOverridesToCRD(p.DrainOverrides),
		ProcessingStrategy:  ProtoProcessingStrategyToCRD(p.ProcessingStrategy),
	}

	if p.EntitiesImpacted != nil {
		spec.EntitiesImpacted = make([]*v1alpha1.Entity, len(p.EntitiesImpacted))
		for i, e := range p.EntitiesImpacted {
			spec.EntitiesImpacted[i] = ProtoEntityToCRD(e)
		}
	}

	if p.GeneratedTimestamp != nil {
		t := metav1.NewTime(p.GeneratedTimestamp.AsTime())
		spec.GeneratedTimestamp = &t
	}

	return spec
}

// CRDSpecToProtoHealthEvent converts a CRD HealthEventSpec back into a protobuf HealthEvent.
// It maps each field from the CRD representation to the protobuf model.
func CRDSpecToProtoHealthEvent(c v1alpha1.HealthEventSpec) *protos.HealthEvent {
	p := &protos.HealthEvent{
		Version:             c.Version,
		Agent:               c.Agent,
		ComponentClass:      c.ComponentClass,
		CheckName:           c.CheckName,
		Message:             c.Message,
		RecommendedAction:   CRDRecommendedActionToProto(c.RecommendedAction),
		ErrorCode:           c.ErrorCode,
		Metadata:            c.Metadata,
		NodeName:            c.NodeName,
		QuarantineOverrides: CRDBehaviourOverridesToProto(c.QuarantineOverrides),
		DrainOverrides:      CRDBehaviourOverridesToProto(c.DrainOverrides),
		ProcessingStrategy:  CRDProcessingStrategyToProto(c.ProcessingStrategy),
	}

	// *bool -> bool (default false if nil)
	if c.IsFatal != nil {
		p.IsFatal = *c.IsFatal
	}

	if c.IsHealthy != nil {
		p.IsHealthy = *c.IsHealthy
	}

	if c.EntitiesImpacted != nil {
		p.EntitiesImpacted = make([]*protos.Entity, len(c.EntitiesImpacted))
		for i, e := range c.EntitiesImpacted {
			p.EntitiesImpacted[i] = CRDEntityToProto(e)
		}
	}

	if c.GeneratedTimestamp != nil {
		p.GeneratedTimestamp = timestamppb.New(c.GeneratedTimestamp.Time)
	}

	return p
}

// RecommendedAction conversions
func ProtoRecommendedActionToCRD(p protos.RecommendedAction) v1alpha1.RecommendedAction {
	return v1alpha1.RecommendedAction(
		protos.RecommendedAction_name[int32(p)],
	)
}

func CRDRecommendedActionToProto(c v1alpha1.RecommendedAction) protos.RecommendedAction {
	return protos.RecommendedAction(
		protos.RecommendedAction_value[string(c)],
	)
}

// ProcessingStrategy conversions
func ProtoProcessingStrategyToCRD(p protos.ProcessingStrategy) v1alpha1.ProcessingStrategy {
	return v1alpha1.ProcessingStrategy(protos.ProcessingStrategy_name[int32(p)])
}

func CRDProcessingStrategyToProto(c v1alpha1.ProcessingStrategy) protos.ProcessingStrategy {
	return protos.ProcessingStrategy(protos.ProcessingStrategy_value[string(c)])
}
