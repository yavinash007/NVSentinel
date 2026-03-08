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
	"sort"

	"github.com/nvidia/nvsentinel/data-models/pkg/model"
	"github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"

	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// KubernetesHealthEventStore implements HealthEventStore using Kubernetes CRs.
// Each health event is stored as a HealthEventResource custom resource where:
//   - spec contains the immutable HealthEvent data
//   - status contains the mutable HealthEventStatus (quarantine, drain, remediation state)
type KubernetesHealthEventStore struct {
	client    crclient.Client
	namespace string
}

// NewKubernetesHealthEventStore creates a new Kubernetes health event store.
func NewKubernetesHealthEventStore(client crclient.Client, namespace string) datastore.HealthEventStore {
	slog.Info("Creating Kubernetes health event store", "namespace", namespace)

	return &KubernetesHealthEventStore{
		client:    client,
		namespace: namespace,
	}
}

// UpdateHealthEventStatus replaces the full status of a HealthEventResource CR.
func (k *KubernetesHealthEventStore) UpdateHealthEventStatus(
	ctx context.Context, id string, status datastore.HealthEventStatus,
) error {
	slog.Debug("UpdateHealthEventStatus called", "id", id, "namespace", k.namespace)

	cr := &model.HealthEventResourceCRD{}
	key := crclient.ObjectKey{Name: id, Namespace: k.namespace}

	if err := k.client.Get(ctx, key, cr); err != nil {
		return fmt.Errorf("failed to get HealthEventResource %s: %w", id, err)
	}

	cr.Status = datastoreStatusToProto(status)

	if err := k.client.Status().Update(ctx, cr); err != nil {
		return fmt.Errorf("failed to update status for %s: %w", id, err)
	}

	slog.Debug("UpdateHealthEventStatus completed", "id", id)

	return nil
}

// UpdateHealthEventStatusByNode updates the status of all CRs matching a given node name.
func (k *KubernetesHealthEventStore) UpdateHealthEventStatusByNode(
	ctx context.Context, nodeName string, status datastore.HealthEventStatus,
) error {
	slog.Debug("UpdateHealthEventStatusByNode called", "node", nodeName, "namespace", k.namespace)

	list := &model.HealthEventResourceCRDList{}
	if err := k.client.List(ctx, list,
		crclient.InNamespace(k.namespace),
		crclient.MatchingLabels{labelNodeName: nodeName},
	); err != nil {
		return fmt.Errorf("failed to list CRs for node %s: %w", nodeName, err)
	}

	slog.Debug("Found CRs for node status update", "node", nodeName, "count", len(list.Items))

	protoStatus := datastoreStatusToProto(status)

	for i := range list.Items {
		cr := &list.Items[i]
		cr.Status = protoStatus

		if err := k.client.Status().Update(ctx, cr); err != nil {
			return fmt.Errorf("failed to update status for CR %s: %w", cr.Name, err)
		}

		slog.Debug("Updated CR status", "crName", cr.Name, "node", nodeName)
	}

	slog.Debug("UpdateHealthEventStatusByNode completed", "node", nodeName, "updatedCount", len(list.Items))

	return nil
}

// FindHealthEventsByNode lists all HealthEventResource CRs for a given node.
func (k *KubernetesHealthEventStore) FindHealthEventsByNode(
	ctx context.Context, nodeName string,
) ([]datastore.HealthEventWithStatus, error) {
	slog.Debug("FindHealthEventsByNode called", "node", nodeName, "namespace", k.namespace)

	list := &model.HealthEventResourceCRDList{}
	if err := k.client.List(ctx, list,
		crclient.InNamespace(k.namespace),
		crclient.MatchingLabels{labelNodeName: nodeName},
	); err != nil {
		return nil, datastore.NewQueryError(datastore.ProviderKubernetes, "failed to list CRs by node", err).
			WithMetadata("nodeName", nodeName)
	}

	events := crListToHealthEvents(list)
	slog.Debug("FindHealthEventsByNode completed", "node", nodeName, "resultCount", len(events))

	return events, nil
}

// FindHealthEventsByFilter lists CRs matching a generic filter map.
// Recognized keys are mapped to label selectors; unrecognized keys are filtered in-memory.
func (k *KubernetesHealthEventStore) FindHealthEventsByFilter(
	ctx context.Context, filter map[string]interface{},
) ([]datastore.HealthEventWithStatus, error) {
	slog.Debug("FindHealthEventsByFilter called", "filter", filter, "namespace", k.namespace)

	labels := buildMatchingLabels(filter)

	list := &model.HealthEventResourceCRDList{}
	opts := []crclient.ListOption{crclient.InNamespace(k.namespace)}

	if len(labels) > 0 {
		opts = append(opts, crclient.MatchingLabels(labels))
		slog.Debug("Using label selectors", "labels", labels)
	}

	if err := k.client.List(ctx, list, opts...); err != nil {
		return nil, datastore.NewQueryError(datastore.ProviderKubernetes, "failed to list CRs by filter", err).
			WithMetadata("filter", filter)
	}

	events := crListToHealthEvents(list)
	filtered := filterInMemory(events, filter)

	slog.Debug("FindHealthEventsByFilter completed",
		"totalCRs", len(list.Items), "afterLabelFilter", len(events), "afterInMemoryFilter", len(filtered))

	return filtered, nil
}

// FindHealthEventsByStatus finds CRs whose quarantine or eviction status matches.
func (k *KubernetesHealthEventStore) FindHealthEventsByStatus(
	ctx context.Context, status datastore.Status,
) ([]datastore.HealthEventWithStatus, error) {
	slog.Debug("FindHealthEventsByStatus called", "status", status, "namespace", k.namespace)

	list := &model.HealthEventResourceCRDList{}
	if err := k.client.List(ctx, list, crclient.InNamespace(k.namespace)); err != nil {
		return nil, datastore.NewQueryError(datastore.ProviderKubernetes, "failed to list CRs", err).
			WithMetadata("status", string(status))
	}

	allEvents := crListToHealthEvents(list)
	statusStr := string(status)

	var matched []datastore.HealthEventWithStatus

	for _, ev := range allEvents {
		if (ev.HealthEventStatus.NodeQuarantined != nil && string(*ev.HealthEventStatus.NodeQuarantined) == statusStr) ||
			string(ev.HealthEventStatus.UserPodsEvictionStatus.Status) == statusStr {
			matched = append(matched, ev)
		}
	}

	slog.Debug("FindHealthEventsByStatus completed",
		"status", status, "totalCRs", len(allEvents), "matched", len(matched))

	return matched, nil
}

// FindHealthEventsByQuery converts the QueryBuilder to a filter map and delegates to FindHealthEventsByFilter.
func (k *KubernetesHealthEventStore) FindHealthEventsByQuery(
	ctx context.Context, builder datastore.QueryBuilder,
) ([]datastore.HealthEventWithStatus, error) {
	filter := builder.ToMongo()
	slog.Debug("FindHealthEventsByQuery called", "filter", filter)

	return k.FindHealthEventsByFilter(ctx, filter)
}

// UpdateHealthEventsByQuery lists matching CRs via the query builder and applies
// the update builder's fields to each CR's status.
func (k *KubernetesHealthEventStore) UpdateHealthEventsByQuery(
	ctx context.Context, queryBuilder datastore.QueryBuilder, updateBuilder datastore.UpdateBuilder,
) error {
	slog.Debug("UpdateHealthEventsByQuery called")

	events, err := k.FindHealthEventsByQuery(ctx, queryBuilder)
	if err != nil {
		return fmt.Errorf("failed to find CRs for update: %w", err)
	}

	updateFields := updateBuilder.ToMongo()
	setFields, _ := updateFields["$set"].(map[string]interface{})

	if setFields == nil {
		return fmt.Errorf("update builder produced no $set fields")
	}

	slog.Debug("UpdateHealthEventsByQuery applying updates",
		"matchedEvents", len(events), "setFields", setFields)

	updatedCount := 0

	for _, ev := range events {
		if ev.RawEvent == nil {
			continue
		}

		crName, _ := ev.RawEvent["crName"].(string)
		if crName == "" {
			slog.Warn("Skipping event with no crName in RawEvent")
			continue
		}

		cr := &model.HealthEventResourceCRD{}
		key := crclient.ObjectKey{Name: crName, Namespace: k.namespace}

		if err := k.client.Get(ctx, key, cr); err != nil {
			return fmt.Errorf("failed to get CR %s: %w", crName, err)
		}

		applySetFieldsToStatus(cr, setFields)

		if err := k.client.Status().Update(ctx, cr); err != nil {
			return fmt.Errorf("failed to update CR %s: %w", crName, err)
		}

		updatedCount++
	}

	slog.Debug("UpdateHealthEventsByQuery completed", "updatedCount", updatedCount)

	return nil
}

// UpdateNodeQuarantineStatus updates the nodeQuarantined field on a CR's status subresource.
func (k *KubernetesHealthEventStore) UpdateNodeQuarantineStatus(
	ctx context.Context, eventID string, status datastore.Status,
) error {
	slog.Debug("UpdateNodeQuarantineStatus called", "eventID", eventID, "status", status)

	cr := &model.HealthEventResourceCRD{}
	key := crclient.ObjectKey{Name: eventID, Namespace: k.namespace}

	if err := k.client.Get(ctx, key, cr); err != nil {
		return fmt.Errorf("failed to get HealthEventResource %s: %w", eventID, err)
	}

	if cr.Status == nil {
		cr.Status = &protos.HealthEventStatus{}
	}

	cr.Status.NodeQuarantined = string(status)

	if status == datastore.Quarantined || status == datastore.AlreadyQuarantined {
		cr.Status.QuarantineFinishTimestamp = timestamppb.Now()
		slog.Debug("Set quarantine finish timestamp", "eventID", eventID)
	}

	if err := k.client.Status().Update(ctx, cr); err != nil {
		return fmt.Errorf("failed to update quarantine status for %s: %w", eventID, err)
	}

	slog.Info("Updated node quarantine status", "eventID", eventID, "status", status)

	return nil
}

// UpdatePodEvictionStatus updates the userPodsEvictionStatus on a CR's status.
func (k *KubernetesHealthEventStore) UpdatePodEvictionStatus(
	ctx context.Context, eventID string, status datastore.OperationStatus,
) error {
	slog.Debug("UpdatePodEvictionStatus called",
		"eventID", eventID, "evictionStatus", status.Status, "message", status.Message)

	cr := &model.HealthEventResourceCRD{}
	key := crclient.ObjectKey{Name: eventID, Namespace: k.namespace}

	if err := k.client.Get(ctx, key, cr); err != nil {
		return fmt.Errorf("failed to get HealthEventResource %s: %w", eventID, err)
	}

	if cr.Status == nil {
		cr.Status = &protos.HealthEventStatus{}
	}

	cr.Status.UserPodsEvictionStatus = &protos.OperationStatus{
		Status:  string(status.Status),
		Message: status.Message,
	}

	if status.Status == datastore.StatusSucceeded || status.Status == datastore.StatusFailed {
		cr.Status.DrainFinishTimestamp = timestamppb.Now()
		slog.Debug("Set drain finish timestamp", "eventID", eventID)
	}

	if err := k.client.Status().Update(ctx, cr); err != nil {
		return fmt.Errorf("failed to update pod eviction status for %s: %w", eventID, err)
	}

	slog.Info("Updated pod eviction status", "eventID", eventID, "status", status.Status)

	return nil
}

// UpdateRemediationStatus updates the faultRemediated field on a CR's status.
func (k *KubernetesHealthEventStore) UpdateRemediationStatus(
	ctx context.Context, eventID string, status interface{},
) error {
	slog.Debug("UpdateRemediationStatus called", "eventID", eventID, "statusType", fmt.Sprintf("%T", status))

	var faultRemediated bool

	switch v := status.(type) {
	case bool:
		faultRemediated = v
	case *bool:
		if v == nil {
			slog.Debug("UpdateRemediationStatus skipped: nil bool pointer", "eventID", eventID)
			return nil
		}

		faultRemediated = *v
	default:
		return fmt.Errorf("invalid remediation status type: %T", status)
	}

	cr := &model.HealthEventResourceCRD{}
	key := crclient.ObjectKey{Name: eventID, Namespace: k.namespace}

	if err := k.client.Get(ctx, key, cr); err != nil {
		return fmt.Errorf("failed to get HealthEventResource %s: %w", eventID, err)
	}

	if cr.Status == nil {
		cr.Status = &protos.HealthEventStatus{}
	}

	cr.Status.FaultRemediated = wrapperspb.Bool(faultRemediated)
	cr.Status.LastRemediationTimestamp = timestamppb.Now()

	if err := k.client.Status().Update(ctx, cr); err != nil {
		return fmt.Errorf("failed to update remediation status for %s: %w", eventID, err)
	}

	slog.Info("Updated remediation status", "eventID", eventID, "faultRemediated", faultRemediated)

	return nil
}

// CheckIfNodeAlreadyDrained checks if any CR for the given node has eviction status Succeeded.
func (k *KubernetesHealthEventStore) CheckIfNodeAlreadyDrained(
	ctx context.Context, nodeName string,
) (bool, error) {
	slog.Debug("CheckIfNodeAlreadyDrained called", "node", nodeName)

	list := &model.HealthEventResourceCRDList{}
	if err := k.client.List(ctx, list,
		crclient.InNamespace(k.namespace),
		crclient.MatchingLabels{labelNodeName: nodeName},
	); err != nil {
		return false, datastore.NewQueryError(datastore.ProviderKubernetes, "failed to list CRs for drain check", err).
			WithMetadata("nodeName", nodeName)
	}

	for i := range list.Items {
		if list.Items[i].Status != nil &&
			list.Items[i].Status.UserPodsEvictionStatus != nil &&
			list.Items[i].Status.UserPodsEvictionStatus.Status == string(datastore.StatusSucceeded) {
			slog.Debug("Node already drained", "node", nodeName, "crName", list.Items[i].Name)
			return true, nil
		}
	}

	slog.Debug("Node not yet drained", "node", nodeName, "crsChecked", len(list.Items))

	return false, nil
}

// FindLatestEventForNode returns the most recently created CR for a given node.
func (k *KubernetesHealthEventStore) FindLatestEventForNode(
	ctx context.Context, nodeName string,
) (*datastore.HealthEventWithStatus, error) {
	slog.Debug("FindLatestEventForNode called", "node", nodeName)

	list := &model.HealthEventResourceCRDList{}
	if err := k.client.List(ctx, list,
		crclient.InNamespace(k.namespace),
		crclient.MatchingLabels{labelNodeName: nodeName},
	); err != nil {
		return nil, datastore.NewQueryError(datastore.ProviderKubernetes, "failed to list CRs for latest event", err).
			WithMetadata("nodeName", nodeName)
	}

	if len(list.Items) == 0 {
		slog.Debug("No events found for node", "node", nodeName)
		return nil, nil
	}

	sort.Slice(list.Items, func(i, j int) bool {
		return list.Items[i].CreationTimestamp.Time.After(list.Items[j].CreationTimestamp.Time)
	})

	latest := crToHealthEvent(&list.Items[0])
	slog.Debug("FindLatestEventForNode completed",
		"node", nodeName, "totalEvents", len(list.Items), "latestCR", list.Items[0].Name)

	return latest, nil
}

// --- helpers ---

// datastoreStatusToProto converts datastore.HealthEventStatus to the protobuf status
// used by the CRD.
func datastoreStatusToProto(status datastore.HealthEventStatus) *protos.HealthEventStatus {
	ps := &protos.HealthEventStatus{
		QuarantineFinishTimestamp: status.QuarantineFinishTimestamp,
		DrainFinishTimestamp:      status.DrainFinishTimestamp,
		LastRemediationTimestamp:  status.LastRemediationTimestamp,
	}

	if status.NodeQuarantined != nil {
		ps.NodeQuarantined = string(*status.NodeQuarantined)
	}

	ps.UserPodsEvictionStatus = &protos.OperationStatus{
		Status:  string(status.UserPodsEvictionStatus.Status),
		Message: status.UserPodsEvictionStatus.Message,
	}

	if status.FaultRemediated != nil {
		ps.FaultRemediated = wrapperspb.Bool(*status.FaultRemediated)
	}

	slog.Debug("Converted datastore status to proto",
		"nodeQuarantined", ps.NodeQuarantined,
		"evictionStatus", ps.UserPodsEvictionStatus.GetStatus(),
		"faultRemediated", ps.FaultRemediated.GetValue())

	return ps
}

// protoStatusToDatastore converts the protobuf CRD status to the datastore abstraction type.
func protoStatusToDatastore(ps *protos.HealthEventStatus) datastore.HealthEventStatus {
	if ps == nil {
		slog.Debug("Proto status is nil, returning empty datastore status")
		return datastore.HealthEventStatus{}
	}

	ds := datastore.HealthEventStatus{
		QuarantineFinishTimestamp: ps.QuarantineFinishTimestamp,
		DrainFinishTimestamp:      ps.DrainFinishTimestamp,
		LastRemediationTimestamp:  ps.LastRemediationTimestamp,
	}

	if ps.NodeQuarantined != "" {
		s := datastore.Status(ps.NodeQuarantined)
		ds.NodeQuarantined = &s
	}

	if ps.UserPodsEvictionStatus != nil {
		ds.UserPodsEvictionStatus = datastore.OperationStatus{
			Status:  datastore.Status(ps.UserPodsEvictionStatus.Status),
			Message: ps.UserPodsEvictionStatus.Message,
		}
	}

	if ps.FaultRemediated != nil {
		b := ps.FaultRemediated.GetValue()
		ds.FaultRemediated = &b
	}

	slog.Debug("Converted proto status to datastore",
		"nodeQuarantined", ps.NodeQuarantined,
		"evictionStatus", ps.UserPodsEvictionStatus.GetStatus(),
		"faultRemediated", ps.FaultRemediated.GetValue())

	return ds
}

// crListToHealthEvents converts a typed CRD list to datastore.HealthEventWithStatus slice.
func crListToHealthEvents(list *model.HealthEventResourceCRDList) []datastore.HealthEventWithStatus {
	events := make([]datastore.HealthEventWithStatus, 0, len(list.Items))

	for i := range list.Items {
		ev := crToHealthEvent(&list.Items[i])
		events = append(events, *ev)
	}

	slog.Debug("Converted CR list to health events", "count", len(events))

	return events
}

// crToHealthEvent converts a single typed CR to a datastore.HealthEventWithStatus.
func crToHealthEvent(cr *model.HealthEventResourceCRD) *datastore.HealthEventWithStatus {
	createdAt := cr.CreationTimestamp.Time
	if ann := cr.GetAnnotations(); ann != nil {
		if ts, ok := ann[originalHealthEventTimestamp]; ok {
			if t, err := parseTime(ts); err == nil {
				createdAt = t
			}
		}
	}

	nodeName := ""
	if cr.Spec != nil {
		nodeName = cr.Spec.NodeName
	}

	slog.Debug("Converted CR to health event", "crName", cr.Name, "node", nodeName, "createdAt", createdAt)

	return &datastore.HealthEventWithStatus{
		CreatedAt:         createdAt,
		HealthEvent:       cr.Spec,
		HealthEventStatus: protoStatusToDatastore(cr.Status),
		RawEvent:          datastore.Event{"crName": cr.Name},
	}
}

// buildMatchingLabels builds a label map from recognized filter keys.
func buildMatchingLabels(filter map[string]interface{}) map[string]string {
	labels := map[string]string{}

	if v, ok := filter["healthevent.nodename"]; ok {
		labels[labelNodeName] = fmt.Sprintf("%v", v)
	}

	if v, ok := filter["healthevent.checkname"]; ok {
		labels[labelCheckName] = fmt.Sprintf("%v", v)
	}

	if v, ok := filter["healthevent.agent"]; ok {
		labels[labelAgent] = fmt.Sprintf("%v", v)
	}

	return labels
}

// filterInMemory applies non-label filter keys by checking the deserialized events.
func filterInMemory(events []datastore.HealthEventWithStatus, filter map[string]interface{}) []datastore.HealthEventWithStatus {
	knownLabelKeys := map[string]bool{
		"healthevent.nodename":  true,
		"healthevent.checkname": true,
		"healthevent.agent":     true,
	}

	hasInMemoryFilters := false

	for key := range filter {
		if !knownLabelKeys[key] {
			hasInMemoryFilters = true
			break
		}
	}

	if !hasInMemoryFilters {
		return events
	}

	slog.Debug("Applying in-memory filters", "inputCount", len(events), "filter", filter)

	var result []datastore.HealthEventWithStatus

	for _, ev := range events {
		if matchesFilter(ev, filter) {
			result = append(result, ev)
		}
	}

	slog.Debug("In-memory filtering completed", "inputCount", len(events), "outputCount", len(result))

	return result
}

// matchesFilter checks whether a HealthEventWithStatus matches all non-label filter criteria.
func matchesFilter(ev datastore.HealthEventWithStatus, filter map[string]interface{}) bool {
	for key, expected := range filter {
		switch key {
		case "healtheventstatus.nodequarantined":
			if ev.HealthEventStatus.NodeQuarantined == nil || string(*ev.HealthEventStatus.NodeQuarantined) != fmt.Sprintf("%v", expected) {
				return false
			}
		case "healtheventstatus.userpodsevictionstatus.status":
			if string(ev.HealthEventStatus.UserPodsEvictionStatus.Status) != fmt.Sprintf("%v", expected) {
				return false
			}
		case "healtheventstatus.faultremediated":
			if ev.HealthEventStatus.FaultRemediated == nil {
				return false
			}

			if fmt.Sprintf("%v", *ev.HealthEventStatus.FaultRemediated) != fmt.Sprintf("%v", expected) {
				return false
			}
		}
	}

	return true
}

// applySetFieldsToStatus applies a map of dot-path $set fields to the CR's proto status.
func applySetFieldsToStatus(cr *model.HealthEventResourceCRD, setFields map[string]interface{}) {
	if cr.Status == nil {
		cr.Status = &protos.HealthEventStatus{}
	}

	slog.Debug("Applying set fields to CR status", "crName", cr.Name, "fields", setFields)

	for dotPath, value := range setFields {
		key := dotPath
		if len(key) > len("healtheventstatus.") && key[:len("healtheventstatus.")] == "healtheventstatus." {
			key = key[len("healtheventstatus."):]
		}

		switch key {
		case "nodequarantined":
			cr.Status.NodeQuarantined = fmt.Sprintf("%v", value)
		case "faultremediated":
			if b, ok := value.(bool); ok {
				cr.Status.FaultRemediated = wrapperspb.Bool(b)
			}
		case "userpodsevictionstatus.status":
			if cr.Status.UserPodsEvictionStatus == nil {
				cr.Status.UserPodsEvictionStatus = &protos.OperationStatus{}
			}

			cr.Status.UserPodsEvictionStatus.Status = fmt.Sprintf("%v", value)
		case "userpodsevictionstatus.message":
			if cr.Status.UserPodsEvictionStatus == nil {
				cr.Status.UserPodsEvictionStatus = &protos.OperationStatus{}
			}

			cr.Status.UserPodsEvictionStatus.Message = fmt.Sprintf("%v", value)
		default:
			slog.Warn("Unknown set field key, skipping", "key", key, "originalPath", dotPath)
		}
	}
}

var _ datastore.HealthEventStore = (*KubernetesHealthEventStore)(nil)
