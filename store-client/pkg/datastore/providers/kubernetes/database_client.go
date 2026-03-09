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
	"time"

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
	client    crclient.WithWatch
	namespace string
}

// NewKubernetesDatabaseClient creates a new Kubernetes database client.
func NewKubernetesDatabaseClient(k8sClient crclient.WithWatch, namespace string) *KubernetesDatabaseClient {
	slog.Info("Creating Kubernetes database client", "namespace", namespace)

	return &KubernetesDatabaseClient{
		client:    k8sClient,
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
	slog.Debug("FindOne called", "filter", filter)

	list := &model.HealthEventResourceCRDList{}
	if err := k.client.List(ctx, list, crclient.InNamespace(k.namespace)); err != nil {
		return nil, fmt.Errorf("failed to list HealthEventResources: %w", err)
	}

	if len(list.Items) == 0 {
		return &k8sEmptySingleResult{}, nil
	}

	cr := &list.Items[0]

	return &k8sSingleResult{cr: cr}, nil
}

func (k *KubernetesDatabaseClient) Find(
	ctx context.Context, filter interface{}, options *client.FindOptions,
) (client.Cursor, error) {
	slog.Debug("Find called", "filter", filter)

	list := &model.HealthEventResourceCRDList{}
	if err := k.client.List(ctx, list, crclient.InNamespace(k.namespace)); err != nil {
		return nil, fmt.Errorf("failed to list HealthEventResources: %w", err)
	}

	items := filterCRsByTime(list.Items, filter)

	if options != nil && options.Limit != nil && *options.Limit > 0 && int64(len(items)) > *options.Limit {
		items = items[:*options.Limit]
	}

	slog.Debug("Find listed CRs", "total", len(list.Items), "afterFilter", len(items))

	return newK8sCursor(items), nil
}

func (k *KubernetesDatabaseClient) CountDocuments(
	ctx context.Context, filter interface{}, _ *client.CountOptions,
) (int64, error) {
	slog.Debug("CountDocuments called", "namespace", k.namespace, "filter", filter)
	list := &model.HealthEventResourceCRDList{}
	if err := k.client.List(ctx, list, crclient.InNamespace(k.namespace)); err != nil {
		return 0, fmt.Errorf("failed to list HealthEventResources: %w", err)
	}
	items := filterCRsByTime(list.Items, filter)
	return int64(len(items)), nil
}

func (k *KubernetesDatabaseClient) Aggregate(
	_ context.Context, pipeline interface{},
) (client.Cursor, error) {
	slog.Debug("Aggregate called on Kubernetes database client", "namespace", k.namespace)
	return &k8sEmptyCursor{}, nil
}

// k8sEmptyCursor implements client.Cursor returning zero results.
// Used by Aggregate until full pipeline evaluation is implemented.
type k8sEmptyCursor struct{}

func (c *k8sEmptyCursor) Next(_ context.Context) bool        { return false }
func (c *k8sEmptyCursor) Decode(_ interface{}) error          { return fmt.Errorf("cursor exhausted") }
func (c *k8sEmptyCursor) Close(_ context.Context) error       { return nil }
func (c *k8sEmptyCursor) All(_ context.Context, _ interface{}) error { return nil }
func (c *k8sEmptyCursor) Err() error                          { return nil }

func (k *KubernetesDatabaseClient) Ping(_ context.Context) error {
	return nil
}

func (k *KubernetesDatabaseClient) NewChangeStreamWatcher(
	ctx context.Context, tokenConfig client.TokenConfig, pipeline interface{},
) (client.ChangeStreamWatcher, error) {
	slog.Info("Creating Kubernetes change stream watcher via DatabaseClient",
		"clientName", tokenConfig.ClientName, "namespace", k.namespace)

	w := NewKubernetesChangeStreamWatcher(k.client, k.namespace, tokenConfig.ClientName)

	return w.Unwrap(), nil
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

// filterCRsByTime applies createdAt time range filtering from a MongoDB-style filter map.
func filterCRsByTime(items []model.HealthEventResourceCRD, filter interface{}) []model.HealthEventResourceCRD {
	filterMap, ok := filter.(map[string]any)
	if !ok {
		return items
	}

	createdAtRaw, ok := filterMap["createdAt"]
	if !ok {
		return items
	}

	rangeMap, ok := createdAtRaw.(map[string]any)
	if !ok {
		return items
	}

	var gte, lt time.Time
	if v, ok := rangeMap["$gte"]; ok {
		if t, ok := v.(time.Time); ok {
			gte = t
		}
	}
	if v, ok := rangeMap["$lt"]; ok {
		if t, ok := v.(time.Time); ok {
			lt = t
		}
	}

	var filtered []model.HealthEventResourceCRD
	for i := range items {
		ts := items[i].CreationTimestamp.Time
		if ann := items[i].GetAnnotations(); ann != nil {
			if raw, ok := ann[originalHealthEventTimestamp]; ok {
				if parsed, err := parseTime(raw); err == nil {
					ts = parsed
				}
			}
		}
		if !gte.IsZero() && ts.Before(gte) {
			continue
		}
		if !lt.IsZero() && !ts.Before(lt) {
			continue
		}
		filtered = append(filtered, items[i])
	}
	return filtered
}

// k8sCursor implements client.Cursor backed by an in-memory slice of CRs.
type k8sCursor struct {
	items []model.HealthEventResourceCRD
	idx   int
	err   error
}

func newK8sCursor(items []model.HealthEventResourceCRD) *k8sCursor {
	return &k8sCursor{items: items, idx: -1}
}

func (c *k8sCursor) Next(_ context.Context) bool {
	c.idx++
	return c.idx < len(c.items)
}

func (c *k8sCursor) Decode(v interface{}) error {
	if c.idx < 0 || c.idx >= len(c.items) {
		return fmt.Errorf("cursor out of bounds")
	}
	return decodeCRInto(&c.items[c.idx], v)
}

func (c *k8sCursor) Close(_ context.Context) error { return nil }

func (c *k8sCursor) All(_ context.Context, results interface{}) error {
	slice, ok := results.(*[]model.HealthEventWithStatus)
	if !ok {
		return fmt.Errorf("All: expected *[]model.HealthEventWithStatus, got %T", results)
	}
	for i := range c.items {
		var hews model.HealthEventWithStatus
		if err := decodeCRInto(&c.items[i], &hews); err != nil {
			return err
		}
		*slice = append(*slice, hews)
	}
	return nil
}

func (c *k8sCursor) Err() error { return c.err }

// k8sSingleResult implements client.SingleResult for a found CR.
type k8sSingleResult struct {
	cr *model.HealthEventResourceCRD
}

func (r *k8sSingleResult) Decode(v interface{}) error {
	return decodeCRInto(r.cr, v)
}

func (r *k8sSingleResult) Err() error { return nil }

// k8sEmptySingleResult implements client.SingleResult for a not-found case.
type k8sEmptySingleResult struct{}

func (r *k8sEmptySingleResult) Decode(_ interface{}) error {
	return fmt.Errorf("no documents in result")
}

func (r *k8sEmptySingleResult) Err() error { return nil }

// decodeCRInto populates the target from a HealthEventResourceCRD.
func decodeCRInto(cr *model.HealthEventResourceCRD, v interface{}) error {
	switch target := v.(type) {
	case *model.HealthEventWithStatus:
		createdAt := cr.CreationTimestamp.Time
		if ann := cr.GetAnnotations(); ann != nil {
			if ts, ok := ann[originalHealthEventTimestamp]; ok {
				if t, err := parseTime(ts); err == nil {
					createdAt = t
				}
			}
		}
		target.CreatedAt = createdAt
		target.HealthEvent = cr.Spec
		target.HealthEventStatus = cr.Status
		return nil
	default:
		return fmt.Errorf("decodeCRInto: unsupported target type %T", v)
	}
}

var _ client.DatabaseClient = (*KubernetesDatabaseClient)(nil)
