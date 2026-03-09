// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"encoding/json"
	"fmt"

	"github.com/nvidia/nvsentinel/data-models/pkg/protos"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// CRD coordinates for HealthEventResource
var (
	HealthEventResourceGVK = schema.GroupVersionKind{
		Group:   "healthevents.dgxc.nvidia.com",
		Version: "v1",
		Kind:    "HealthEventResource",
	}

	SchemeGroupVersion = schema.GroupVersion{
		Group:   HealthEventResourceGVK.Group,
		Version: HealthEventResourceGVK.Version,
	}
)

// HealthEventResourceCRD is the Kubernetes CRD type for HealthEventResource.
// Spec and Status are generated from the proto definitions.
type HealthEventResourceCRD struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              *protos.HealthEvent       `json:"spec,omitempty"`
	Status            *protos.HealthEventStatus `json:"status,omitempty"`
}

// MarshalJSON uses protojson for Spec/Status so that protobuf well-known types
// (Timestamp → RFC3339 string, BoolValue → plain bool) match the CRD schema.
// Without this, encoding/json serializes Timestamp as {"seconds":...,"nanos":...}
// which the CRD rejects as "must be of type string".
func (in HealthEventResourceCRD) MarshalJSON() ([]byte, error) {
	type helper struct {
		metav1.TypeMeta   `json:",inline"`
		metav1.ObjectMeta `json:"metadata,omitempty"`
		Spec              json.RawMessage `json:"spec,omitempty"`
		Status            json.RawMessage `json:"status,omitempty"`
	}

	h := helper{
		TypeMeta:   in.TypeMeta,
		ObjectMeta: in.ObjectMeta,
	}

	opts := protojson.MarshalOptions{UseEnumNumbers: true}

	if in.Spec != nil {
		b, err := opts.Marshal(in.Spec)
		if err != nil {
			return nil, fmt.Errorf("protojson marshal spec: %w", err)
		}
		h.Spec = b
	}

	if in.Status != nil {
		b, err := opts.Marshal(in.Status)
		if err != nil {
			return nil, fmt.Errorf("protojson marshal status: %w", err)
		}
		h.Status = b
	}

	return json.Marshal(h)
}

// UnmarshalJSON uses protojson for Spec/Status to correctly parse RFC3339 timestamps
// and other protobuf well-known types back into their Go protobuf representations.
func (in *HealthEventResourceCRD) UnmarshalJSON(data []byte) error {
	type helper struct {
		metav1.TypeMeta   `json:",inline"`
		metav1.ObjectMeta `json:"metadata,omitempty"`
		Spec              json.RawMessage `json:"spec,omitempty"`
		Status            json.RawMessage `json:"status,omitempty"`
	}

	var h helper
	if err := json.Unmarshal(data, &h); err != nil {
		return err
	}

	in.TypeMeta = h.TypeMeta
	in.ObjectMeta = h.ObjectMeta

	opts := protojson.UnmarshalOptions{DiscardUnknown: true}

	if len(h.Spec) > 0 {
		in.Spec = &protos.HealthEvent{}
		if err := opts.Unmarshal(h.Spec, in.Spec); err != nil {
			return fmt.Errorf("protojson unmarshal spec: %w", err)
		}
	}

	if len(h.Status) > 0 {
		in.Status = &protos.HealthEventStatus{}
		if err := opts.Unmarshal(h.Status, in.Status); err != nil {
			return fmt.Errorf("protojson unmarshal status: %w", err)
		}
	}

	return nil
}

func (in *HealthEventResourceCRD) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}

	out := &HealthEventResourceCRD{}
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)

	if in.Spec != nil {
		out.Spec = proto.Clone(in.Spec).(*protos.HealthEvent)
	}

	if in.Status != nil {
		out.Status = proto.Clone(in.Status).(*protos.HealthEventStatus)
	}

	return out
}

// HealthEventResourceCRDList is the list type for HealthEventResourceCRD.
type HealthEventResourceCRDList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HealthEventResourceCRD `json:"items"`
}

func (in *HealthEventResourceCRDList) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}

	out := &HealthEventResourceCRDList{}
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)

	if in.Items != nil {
		out.Items = make([]HealthEventResourceCRD, len(in.Items))
		for i := range in.Items {
			cp := in.Items[i].DeepCopyObject().(*HealthEventResourceCRD)
			out.Items[i] = *cp
		}
	}

	return out
}

// AddToScheme registers the HealthEventResource types with the given scheme.
// We use AddKnownTypeWithName because the Go struct is named HealthEventResourceCRD
// but the CRD kind is HealthEventResource (without the CRD suffix).
func AddToScheme(scheme *runtime.Scheme) error {
	scheme.AddKnownTypeWithName(
		SchemeGroupVersion.WithKind("HealthEventResource"),
		&HealthEventResourceCRD{},
	)
	scheme.AddKnownTypeWithName(
		SchemeGroupVersion.WithKind("HealthEventResourceList"),
		&HealthEventResourceCRDList{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)

	return nil
}
