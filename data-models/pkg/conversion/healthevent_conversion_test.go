package conversion

import (
	"reflect"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/nvidia/nvsentinel/data-models/api/v1alpha1"
	"github.com/nvidia/nvsentinel/data-models/pkg/model"
	"github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// HealthEventSpecIgnoredFields are fields that are intentionally NOT part of CRD Spec
var HealthEventSpecIgnoredFields = map[string]struct{}{}

// HealthEventStatusIgnoredFields are fields intentionally NOT part of CRD Status
var HealthEventStatusIgnoredFields = map[string]struct{}{}

// These tests verify that the model structs and the CRD API structs remain in sync.
// They fail if any new fields are added to one side but not the other.
//
// The comparison is recursive: nested structs are also validated so that all
// fields in nested types must match between the model and the API.
func TestHealthEventSpec_NestedFieldParity(t *testing.T) {
	assertRecursiveFieldParity(t, "Proto HealthEvent", reflect.TypeOf(protos.HealthEvent{}), "CRD HealthEventSpec",
		reflect.TypeOf(v1alpha1.HealthEventSpec{}), HealthEventSpecIgnoredFields)
}

func TestHealthEventStatus_NestedFieldParity(t *testing.T) {
	assertRecursiveFieldParity(t, "Model HealthEventStatus", reflect.TypeOf(model.HealthEventStatus{}), "CRD HealthEventStatus",
		reflect.TypeOf(v1alpha1.HealthEventStatus{}), HealthEventStatusIgnoredFields)
}

func exportedFieldPathsRecursive(t reflect.Type, prefix string, ignoreTopLevel map[string]struct{}) map[string]struct{} {

	out := make(map[string]struct{})

	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return out
	}

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)

		if f.PkgPath != "" {
			continue
		}

		// Ignore top-level fields if requested
		if prefix == "" {
			if _, ignored := ignoreTopLevel[f.Name]; ignored {
				continue
			}
		}

		name := f.Name
		path := name
		if prefix != "" {
			path = prefix + "." + name
		}

		out[path] = struct{}{}

		ft := f.Type
		for ft.Kind() == reflect.Ptr || ft.Kind() == reflect.Slice || ft.Kind() == reflect.Map {
			ft = ft.Elem()
		}

		// Recurse into nested structs (excluding scalar structs)
		if ft.Kind() == reflect.Struct && !isScalarStruct(ft) {
			for sub := range exportedFieldPathsRecursive(ft, path, nil) {
				out[sub] = struct{}{}
			}
		}
	}

	return out
}

func assertRecursiveFieldParity(t *testing.T, leftName string, leftType reflect.Type, rightName string, rightType reflect.Type, ignoreTopLevel map[string]struct{}) {
	left := exportedFieldPathsRecursive(leftType, "", ignoreTopLevel)
	right := exportedFieldPathsRecursive(rightType, "", ignoreTopLevel)

	var missingFromLeft []string
	var missingFromRight []string

	for f := range right {
		if _, ok := left[f]; !ok {
			missingFromLeft = append(missingFromLeft, f)
		}
	}

	for f := range left {
		if _, ok := right[f]; !ok {
			missingFromRight = append(missingFromRight, f)
		}
	}

	if len(missingFromLeft) > 0 || len(missingFromRight) > 0 {
		var msg []string

		if len(missingFromLeft) > 0 {
			msg = append(msg,
				leftName+" missing fields:\n  - "+
					strings.Join(missingFromLeft, "\n  - "))
		}

		if len(missingFromRight) > 0 {
			msg = append(msg,
				rightName+" missing fields:\n  - "+
					strings.Join(missingFromRight, "\n  - "))
		}

		t.Fatalf("%s", strings.Join(msg, "\n\n"))
	}

	for path := range left {
		lt, lok := fieldTypeByPath(leftType, path)
		rt, rok := fieldTypeByPath(rightType, path)

		if !lok || !rok {
			continue
		}

		if isScalarStruct(lt) != isScalarStruct(rt) {
			t.Fatalf(
				"field %q scalar mismatch: %s (%s) vs %s (%s)",
				path,
				leftName, lt.String(),
				rightName, rt.String(),
			)
		}
	}

}

func isScalarStruct(t reflect.Type) bool {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return true
	}

	switch t.PkgPath() + "." + t.Name() {
	case "time.Time",
		"k8s.io/apimachinery/pkg/apis/meta/v1.Time",
		"google.golang.org/protobuf/types/known/timestamppb.Timestamp":
		return true
	}

	return false
}

func fieldTypeByPath(root reflect.Type, path string) (reflect.Type, bool) {
	for root.Kind() == reflect.Ptr {
		root = root.Elem()
	}

	parts := strings.Split(path, ".")
	curr := root

	for _, p := range parts {
		if curr.Kind() != reflect.Struct {
			return nil, false
		}

		f, ok := curr.FieldByName(p)
		if !ok {
			return nil, false
		}

		curr = f.Type
		for curr.Kind() == reflect.Ptr || curr.Kind() == reflect.Slice || curr.Kind() == reflect.Map {
			curr = curr.Elem()
		}
	}

	return curr, true
}

func TestHealthEventSpecConversion(t *testing.T) {
	now := time.Now()

	protoEvent := &protos.HealthEvent{
		Version:            1,
		Agent:              "agent1",
		ComponentClass:     "classA",
		CheckName:          "check1",
		IsFatal:            true,
		IsHealthy:          false,
		Message:            "message",
		RecommendedAction:  protos.RecommendedAction_RESTART_VM,
		ErrorCode:          []string{"E1", "E2"},
		Metadata:           map[string]string{"k1": "v1"},
		GeneratedTimestamp: timestamppb.New(now),
		NodeName:           "node-1",
		QuarantineOverrides: &protos.BehaviourOverrides{
			Force: true,
			Skip:  false,
		},
		DrainOverrides: &protos.BehaviourOverrides{
			Force: false,
			Skip:  true,
		},
		EntitiesImpacted: []*protos.Entity{
			{EntityType: "node", EntityValue: "node-1"},
		},
		ProcessingStrategy: protos.ProcessingStrategy_EXECUTE_REMEDIATION,
	}

	// Proto -> CRD
	crdSpec := ProtoHealthEventToCRDSpec(protoEvent)

	if crdSpec.Version != protoEvent.Version ||
		crdSpec.Agent != protoEvent.Agent ||
		crdSpec.ComponentClass != protoEvent.ComponentClass ||
		crdSpec.CheckName != protoEvent.CheckName ||
		*crdSpec.IsFatal != protoEvent.IsFatal ||
		*crdSpec.IsHealthy != protoEvent.IsHealthy ||
		crdSpec.Message != protoEvent.Message ||
		crdSpec.RecommendedAction != v1alpha1.RecommendedAction(protos.RecommendedAction_name[int32(protoEvent.RecommendedAction)]) ||
		crdSpec.ProcessingStrategy != v1alpha1.ProcessingStrategy(protos.ProcessingStrategy_name[int32(protoEvent.ProcessingStrategy)]) {
		t.Fatalf("proto -> crd conversion failed")
	}

	if crdSpec.Metadata["k1"] != "v1" ||
		len(crdSpec.ErrorCode) != 2 ||
		len(crdSpec.EntitiesImpacted) != 1 ||
		crdSpec.EntitiesImpacted[0].EntityType != "node" ||
		crdSpec.EntitiesImpacted[0].EntityValue != "node-1" {
		t.Fatalf("proto -> crd nested conversion failed")
	}

	if crdSpec.GeneratedTimestamp == nil || !crdSpec.GeneratedTimestamp.Time.Equal(now) {
		t.Fatalf("timestamp conversion failed")
	}

	// CRD -> Proto
	backProto := CRDSpecToProtoHealthEvent(crdSpec)

	if backProto.Version != protoEvent.Version ||
		backProto.Agent != protoEvent.Agent ||
		backProto.ComponentClass != protoEvent.ComponentClass ||
		backProto.CheckName != protoEvent.CheckName ||
		backProto.IsFatal != protoEvent.IsFatal ||
		backProto.IsHealthy != protoEvent.IsHealthy ||
		backProto.Message != protoEvent.Message ||
		backProto.RecommendedAction != protoEvent.RecommendedAction ||
		backProto.ProcessingStrategy != protoEvent.ProcessingStrategy {
		t.Fatalf("crd -> proto conversion failed")
	}

	if backProto.Metadata["k1"] != "v1" ||
		len(backProto.ErrorCode) != 2 ||
		len(backProto.EntitiesImpacted) != 1 ||
		backProto.EntitiesImpacted[0].EntityType != "node" ||
		backProto.EntitiesImpacted[0].EntityValue != "node-1" {
		t.Fatalf("crd -> proto nested conversion failed")
	}

	if backProto.GeneratedTimestamp == nil || !backProto.GeneratedTimestamp.AsTime().Equal(now) {
		t.Fatalf("timestamp conversion failed")
	}
}

func TestHealthEventStatusConversion(t *testing.T) {
	now := time.Now()

	modelStatus := &model.HealthEventStatus{
		NodeQuarantined: func() *model.Status {
			s := model.Quarantined
			return &s
		}(),
		UserPodsEvictionStatus: model.OperationStatus{
			Status:  model.StatusInProgress,
			Message: "eviction running",
		},
		FaultRemediated: func() *bool {
			b := true
			return &b
		}(),
		LastRemediationTimestamp: &now,
	}

	// Model -> CRD
	crdStatus := ModelHealthEventStatusToCRD(modelStatus)

	if crdStatus.NodeQuarantined == nil || *crdStatus.NodeQuarantined != v1alpha1.StatusQuarantined {
		t.Fatalf("node quarantine conversion failed")
	}

	if crdStatus.UserPodsEvictionStatus.Status != v1alpha1.StatusInProgress ||
		crdStatus.UserPodsEvictionStatus.Message != "eviction running" {
		t.Fatalf("operation status conversion failed")
	}

	if crdStatus.FaultRemediated == nil || !*crdStatus.FaultRemediated {
		t.Fatalf("fault remediation conversion failed")
	}

	if crdStatus.LastRemediationTimestamp == nil ||
		!crdStatus.LastRemediationTimestamp.Time.Equal(now) {
		t.Fatalf("timestamp conversion failed")
	}

	// CRD -> Model
	backModel := CRDHealthEventStatusToModel(crdStatus)

	if backModel.NodeQuarantined == nil || *backModel.NodeQuarantined != model.Quarantined {
		t.Fatalf("node quarantine reverse conversion failed")
	}

	if backModel.UserPodsEvictionStatus.Status != model.StatusInProgress ||
		backModel.UserPodsEvictionStatus.Message != "eviction running" {
		t.Fatalf("operation status reverse conversion failed")
	}

	if backModel.FaultRemediated == nil || !*backModel.FaultRemediated {
		t.Fatalf("fault remediation reverse conversion failed")
	}

	if backModel.LastRemediationTimestamp == nil ||
		!backModel.LastRemediationTimestamp.Equal(now) {
		t.Fatalf("timestamp reverse conversion failed")
	}
}
