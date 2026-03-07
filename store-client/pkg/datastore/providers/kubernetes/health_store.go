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

package kubernetes

import (
	"context"
	"fmt"

	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"

	"k8s.io/client-go/dynamic"
)

// KubernetesHealthEventStore implements HealthEventStore using Kubernetes CRs.
// Each health event is stored as a HealthEventResource custom resource where:
//   - spec contains the immutable HealthEvent data
//   - status contains the mutable HealthEventStatus (quarantine, drain, remediation state)
type KubernetesHealthEventStore struct {
	dynamicClient dynamic.Interface
	namespace     string
}

// NewKubernetesHealthEventStore creates a new Kubernetes health event store.
func NewKubernetesHealthEventStore(dynamicClient dynamic.Interface, namespace string) datastore.HealthEventStore {
	return &KubernetesHealthEventStore{
		dynamicClient: dynamicClient,
		namespace:     namespace,
	}
}

func (k *KubernetesHealthEventStore) UpdateHealthEventStatus(
	ctx context.Context, id string, status datastore.HealthEventStatus,
) error {
	return fmt.Errorf("kubernetes: UpdateHealthEventStatus not yet implemented")
}

func (k *KubernetesHealthEventStore) UpdateHealthEventStatusByNode(
	ctx context.Context, nodeName string, status datastore.HealthEventStatus,
) error {
	return fmt.Errorf("kubernetes: UpdateHealthEventStatusByNode not yet implemented")
}

func (k *KubernetesHealthEventStore) FindHealthEventsByNode(
	ctx context.Context, nodeName string,
) ([]datastore.HealthEventWithStatus, error) {
	return nil, fmt.Errorf("kubernetes: FindHealthEventsByNode not yet implemented")
}

func (k *KubernetesHealthEventStore) FindHealthEventsByFilter(
	ctx context.Context, filter map[string]interface{},
) ([]datastore.HealthEventWithStatus, error) {
	return nil, fmt.Errorf("kubernetes: FindHealthEventsByFilter not yet implemented")
}

func (k *KubernetesHealthEventStore) FindHealthEventsByStatus(
	ctx context.Context, status datastore.Status,
) ([]datastore.HealthEventWithStatus, error) {
	return nil, fmt.Errorf("kubernetes: FindHealthEventsByStatus not yet implemented")
}

func (k *KubernetesHealthEventStore) FindHealthEventsByQuery(
	ctx context.Context, builder datastore.QueryBuilder,
) ([]datastore.HealthEventWithStatus, error) {
	return nil, fmt.Errorf("kubernetes: FindHealthEventsByQuery not yet implemented")
}

func (k *KubernetesHealthEventStore) UpdateHealthEventsByQuery(
	ctx context.Context, queryBuilder datastore.QueryBuilder, updateBuilder datastore.UpdateBuilder,
) error {
	return fmt.Errorf("kubernetes: UpdateHealthEventsByQuery not yet implemented")
}

func (k *KubernetesHealthEventStore) UpdateNodeQuarantineStatus(
	ctx context.Context, eventID string, status datastore.Status,
) error {
	return fmt.Errorf("kubernetes: UpdateNodeQuarantineStatus not yet implemented")
}

func (k *KubernetesHealthEventStore) UpdatePodEvictionStatus(
	ctx context.Context, eventID string, status datastore.OperationStatus,
) error {
	return fmt.Errorf("kubernetes: UpdatePodEvictionStatus not yet implemented")
}

func (k *KubernetesHealthEventStore) UpdateRemediationStatus(
	ctx context.Context, eventID string, status interface{},
) error {
	return fmt.Errorf("kubernetes: UpdateRemediationStatus not yet implemented")
}

func (k *KubernetesHealthEventStore) CheckIfNodeAlreadyDrained(
	ctx context.Context, nodeName string,
) (bool, error) {
	return false, fmt.Errorf("kubernetes: CheckIfNodeAlreadyDrained not yet implemented")
}

func (k *KubernetesHealthEventStore) FindLatestEventForNode(
	ctx context.Context, nodeName string,
) (*datastore.HealthEventWithStatus, error) {
	return nil, fmt.Errorf("kubernetes: FindLatestEventForNode not yet implemented")
}

var _ datastore.HealthEventStore = (*KubernetesHealthEventStore)(nil)
