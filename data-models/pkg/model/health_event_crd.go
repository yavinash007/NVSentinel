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
	"github.com/nvidia/nvsentinel/data-models/pkg/protos"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
func AddToScheme(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&HealthEventResourceCRD{},
		&HealthEventResourceCRDList{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)

	return nil
}
