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
	"log/slog"
	"strings"

	"github.com/nvidia/nvsentinel/data-models/pkg/model"
	"github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"github.com/nvidia/nvsentinel/store-client/pkg/client"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// KubernetesDatabaseClient implements client.DatabaseClient using Kubernetes CRs.
// This provides backward compatibility with consumers that still use the legacy
// DatabaseClient interface (platform-connector InsertMany, fault-quarantine queries, etc.).
type KubernetesDatabaseClient struct {
	client    crclient.Client
	namespace string
}

// NewKubernetesDatabaseClient creates a new Kubernetes database client.
func NewKubernetesDatabaseClient(client crclient.Client, namespace string) *KubernetesDatabaseClient {
	slog.Info("Creating Kubernetes database client", "namespace", namespace)

	return &KubernetesDatabaseClient{
		client:    client,
		namespace: namespace,
	}
}

func (k *KubernetesDatabaseClient) InsertMany(
	ctx context.Context, documents []interface{},
) (*client.InsertManyResult, error) {
	slog.Info("InsertMany called", "documentCount", len(documents), "namespace", k.namespace)

	insertedIDs := make([]interface{}, 0, len(documents))

	for i, doc := range documents {
		cr, err := healthEventToCR(doc, k.namespace)
		if err != nil {
			slog.Error("Failed to convert document to CR", "index", i, "error", err)
			return nil, fmt.Errorf("failed to convert document %d: %w", i, err)
		}

		if err := k.client.Create(ctx, cr); err != nil {
			slog.Error("Failed to create HealthEventResource CR", "index", i, "generateName", cr.GenerateName, "error", err)
			return nil, fmt.Errorf("failed to create HealthEventResource %d: %w", i, err)
		}

		insertedIDs = append(insertedIDs, cr.Name)
		slog.Debug("Created HealthEventResource CR", "name", cr.Name, "node", cr.Labels[labelNodeName])
	}

	slog.Info("InsertMany completed", "insertedCount", len(insertedIDs))

	return &client.InsertManyResult{InsertedIDs: insertedIDs}, nil
}

// healthEventToCR converts a model.HealthEventWithStatus into a typed HealthEventResourceCRD.
func healthEventToCR(doc interface{}, namespace string) (*model.HealthEventResourceCRD, error) {
	hews, ok := doc.(model.HealthEventWithStatus)
	if !ok {
		return nil, fmt.Errorf("expected model.HealthEventWithStatus, got %T", doc)
	}

	labels := map[string]string{}
	if hews.HealthEvent != nil {
		if hews.HealthEvent.NodeName != "" {
			labels[labelNodeName] = hews.HealthEvent.NodeName
		}

		if hews.HealthEvent.CheckName != "" {
			labels[labelCheckName] = hews.HealthEvent.CheckName
		}

		if hews.HealthEvent.Agent != "" {
			labels[labelAgent] = hews.HealthEvent.Agent
		}
	}

	namePrefix := generateCRNamePrefix(hews.HealthEvent)

	slog.Debug("Converting health event to CR",
		"namePrefix", namePrefix, "namespace", namespace,
		"node", labels[labelNodeName], "check", labels[labelCheckName], "agent", labels[labelAgent])

	cr := &model.HealthEventResourceCRD{
		TypeMeta: metav1.TypeMeta{
			APIVersion: model.SchemeGroupVersion.Group + "/" + model.SchemeGroupVersion.Version,
			Kind:       "HealthEventResource",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: namePrefix,
			Namespace:    namespace,
			Labels:       labels,
			Annotations: map[string]string{
				originalHealthEventTimestamp: hews.CreatedAt.Format("2006-01-02T15:04:05Z"),
			},
		},
		Spec:   hews.HealthEvent,
		Status: hews.HealthEventStatus,
	}

	return cr, nil
}

// generateCRNamePrefix builds a human-readable prefix for GenerateName.
// The K8s API server appends a random suffix to guarantee uniqueness.
func generateCRNamePrefix(he *protos.HealthEvent) string {
	if he == nil {
		return "he-"
	}

	nodeName := strings.ReplaceAll(he.NodeName, ".", "-")
	if len(nodeName) > 40 {
		nodeName = nodeName[:40]
	}

	return fmt.Sprintf("he-%s-", strings.ToLower(nodeName))
}

func (k *KubernetesDatabaseClient) UpdateDocumentStatus(
	ctx context.Context, documentID string, statusPath string, status interface{},
) error {
	slog.Warn("UpdateDocumentStatus called but not implemented", "documentID", documentID, "statusPath", statusPath)
	return fmt.Errorf("kubernetes: UpdateDocumentStatus not yet implemented")
}

func (k *KubernetesDatabaseClient) UpdateDocumentStatusFields(
	ctx context.Context, documentID string, fields map[string]interface{},
) error {
	slog.Debug("UpdateDocumentStatusFields called", "documentID", documentID, "fields", fields)

	cr := &model.HealthEventResourceCRD{}
	key := crclient.ObjectKey{Name: documentID, Namespace: k.namespace}

	if err := k.client.Get(ctx, key, cr); err != nil {
		slog.Error("Failed to get CR for status update", "documentID", documentID, "error", err)
		return fmt.Errorf("failed to get HealthEventResource %s: %w", documentID, err)
	}

	applySetFieldsToStatus(cr, fields)

	if err := k.client.Status().Update(ctx, cr); err != nil {
		slog.Error("Failed to update CR status subresource", "documentID", documentID, "error", err)
		return fmt.Errorf("failed to update status subresource for %s: %w", documentID, err)
	}

	slog.Debug("UpdateDocumentStatusFields completed", "documentID", documentID)

	return nil
}

func (k *KubernetesDatabaseClient) UpdateDocument(
	ctx context.Context, filter interface{}, update interface{},
) (*client.UpdateResult, error) {
	slog.Warn("UpdateDocument called but not implemented")
	return nil, fmt.Errorf("kubernetes: UpdateDocument not yet implemented")
}

func (k *KubernetesDatabaseClient) UpdateManyDocuments(
	ctx context.Context, filter interface{}, update interface{},
) (*client.UpdateResult, error) {
	slog.Warn("UpdateManyDocuments called but not implemented")
	return nil, fmt.Errorf("kubernetes: UpdateManyDocuments not yet implemented")
}

func (k *KubernetesDatabaseClient) UpsertDocument(
	ctx context.Context, filter interface{}, document interface{},
) (*client.UpdateResult, error) {
	slog.Warn("UpsertDocument called but not implemented")
	return nil, fmt.Errorf("kubernetes: UpsertDocument not yet implemented")
}

func (k *KubernetesDatabaseClient) FindOne(
	ctx context.Context, filter interface{}, options *client.FindOneOptions,
) (client.SingleResult, error) {
	slog.Warn("FindOne called but not implemented", "filter", filter)
	return nil, fmt.Errorf("kubernetes: FindOne not yet implemented")
}

func (k *KubernetesDatabaseClient) Find(
	ctx context.Context, filter interface{}, options *client.FindOptions,
) (client.Cursor, error) {
	slog.Warn("Find called but not implemented", "filter", filter)
	return nil, fmt.Errorf("kubernetes: Find not yet implemented")
}

func (k *KubernetesDatabaseClient) CountDocuments(
	ctx context.Context, filter interface{}, options *client.CountOptions,
) (int64, error) {
	slog.Warn("CountDocuments called but not implemented", "filter", filter)
	return 0, fmt.Errorf("kubernetes: CountDocuments not yet implemented")
}

func (k *KubernetesDatabaseClient) Aggregate(
	ctx context.Context, pipeline interface{},
) (client.Cursor, error) {
	slog.Warn("Aggregate called but not implemented")
	return nil, fmt.Errorf("kubernetes: Aggregate not yet implemented")
}

func (k *KubernetesDatabaseClient) Ping(ctx context.Context) error {
	slog.Warn("DatabaseClient Ping called but not implemented")
	return fmt.Errorf("kubernetes: Ping not yet implemented")
}

func (k *KubernetesDatabaseClient) NewChangeStreamWatcher(
	ctx context.Context, tokenConfig client.TokenConfig, pipeline interface{},
) (client.ChangeStreamWatcher, error) {
	slog.Warn("NewChangeStreamWatcher called but not implemented")
	return nil, fmt.Errorf("kubernetes: NewChangeStreamWatcher not yet implemented")
}

func (k *KubernetesDatabaseClient) DeleteResumeToken(
	ctx context.Context, tokenConfig client.TokenConfig,
) error {
	slog.Warn("DeleteResumeToken called but not implemented")
	return fmt.Errorf("kubernetes: DeleteResumeToken not yet implemented")
}

func (k *KubernetesDatabaseClient) Close(_ context.Context) error {
	slog.Debug("Kubernetes database client closed")
	return nil
}

var _ client.DatabaseClient = (*KubernetesDatabaseClient)(nil)
