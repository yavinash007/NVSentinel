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
	"time"

	"github.com/nvidia/nvsentinel/data-models/pkg/model"
	"github.com/nvidia/nvsentinel/store-client/pkg/client"
	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"

	"k8s.io/apimachinery/pkg/watch"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// KubernetesChangeStreamWatcher implements both datastore.ChangeStreamWatcher and
// provides Unwrap() to return a client.ChangeStreamWatcher. This dual-interface
// support is required because fault-quarantine, node-drainer, and fault-remediation
// unwrap the abstract watcher to access the legacy client.ChangeStreamWatcher.Events() channel.
//
// Under the hood, this uses a controller-runtime WithWatch client to watch typed
// HealthEventResourceCRD objects.
type KubernetesChangeStreamWatcher struct {
	k8sClient  crclient.WithWatch
	namespace  string
	clientName string

	eventChan    chan datastore.EventWithToken
	oldEventChan chan client.Event
	stopCh       chan struct{}
}

// NewKubernetesChangeStreamWatcher creates a new Kubernetes change stream watcher.
func NewKubernetesChangeStreamWatcher(
	k8sClient crclient.WithWatch, namespace string, clientName string,
) *KubernetesChangeStreamWatcher {
	slog.Info("Creating Kubernetes change stream watcher", "clientName", clientName, "namespace", namespace)

	return &KubernetesChangeStreamWatcher{
		k8sClient:    k8sClient,
		namespace:    namespace,
		clientName:   clientName,
		eventChan:    make(chan datastore.EventWithToken),
		oldEventChan: make(chan client.Event),
		stopCh:       make(chan struct{}),
	}
}

// Events returns the abstract event channel (datastore.ChangeStreamWatcher interface).
func (w *KubernetesChangeStreamWatcher) Events() <-chan datastore.EventWithToken {
	return w.eventChan
}

// Start begins watching HealthEventResource CRs via the controller-runtime typed client.
// It translates K8s watch events into the same format that MongoDB change streams
// produce, so downstream consumers (fault-quarantine EventWatcher) work unchanged.
func (w *KubernetesChangeStreamWatcher) Start(ctx context.Context) {
	slog.Info("Starting Kubernetes change stream watcher", "clientName", w.clientName, "namespace", w.namespace)

	go func() {
		defer close(w.oldEventChan)
		defer close(w.eventChan)

		const (
			initialBackoff = 1 * time.Second
			maxBackoff     = 30 * time.Second
		)
		backoff := initialBackoff

		for {
			select {
			case <-ctx.Done():
				slog.Info("Kubernetes watcher context cancelled", "clientName", w.clientName)
				return
			case <-w.stopCh:
				slog.Info("Kubernetes watcher stop signal received", "clientName", w.clientName)
				return
			default:
			}

			slog.Debug("Establishing Kubernetes watch", "clientName", w.clientName, "namespace", w.namespace)

			list := &model.HealthEventResourceCRDList{}
			watcher, err := w.k8sClient.Watch(ctx, list, crclient.InNamespace(w.namespace))
			if err != nil {
				slog.Error("Failed to start Kubernetes watch, will retry", "error", err, "clientName", w.clientName, "retryIn", backoff)
				select {
				case <-time.After(backoff):
				case <-ctx.Done():
					return
				case <-w.stopCh:
					return
				}
				backoff = min(backoff*2, maxBackoff)
				continue
			}

			backoff = initialBackoff
			slog.Info("Kubernetes watch established", "clientName", w.clientName)
			w.processWatchEvents(ctx, watcher)
			watcher.Stop()
			slog.Info("Kubernetes watch ended, will re-establish", "clientName", w.clientName)
		}
	}()
}

func (w *KubernetesChangeStreamWatcher) processWatchEvents(
	ctx context.Context, watcher watch.Interface,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		case watchEvent, ok := <-watcher.ResultChan():
			if !ok {
				slog.Warn("Kubernetes watch channel closed, will re-establish", "clientName", w.clientName)
				return
			}

			if watchEvent.Type == watch.Error {
				slog.Error("Kubernetes watch error event", "object", watchEvent.Object)
				return
			}

			if watchEvent.Type != watch.Added && watchEvent.Type != watch.Modified {
				slog.Debug("Ignoring watch event type", "type", watchEvent.Type, "clientName", w.clientName)
				continue
			}

			cr, ok := watchEvent.Object.(*model.HealthEventResourceCRD)
			if !ok {
				slog.Error("Watch event object is not HealthEventResourceCRD", "type", fmt.Sprintf("%T", watchEvent.Object))
				continue
			}

			nodeName := ""
			if cr.Spec != nil {
				nodeName = cr.Spec.NodeName
			}

			slog.Info("Received watch event",
				"type", watchEvent.Type, "crName", cr.Name,
				"node", nodeName, "resourceVersion", cr.ResourceVersion,
				"clientName", w.clientName)

			k8sEvent := &kubernetesEvent{cr: cr}

			select {
			case w.oldEventChan <- k8sEvent:
				slog.Debug("Watch event dispatched to legacy channel", "crName", cr.Name)
			case <-ctx.Done():
				return
			}
		}
	}
}

// MarkProcessed is a no-op for Kubernetes. K8s watches are position-based via
// resourceVersion, which is tracked automatically.
func (w *KubernetesChangeStreamWatcher) MarkProcessed(_ context.Context, _ []byte) error {
	return nil
}

// Close stops the watcher and closes channels.
func (w *KubernetesChangeStreamWatcher) Close(_ context.Context) error {
	slog.Info("Closing Kubernetes change stream watcher", "clientName", w.clientName)

	select {
	case <-w.stopCh:
	default:
		close(w.stopCh)
	}

	return nil
}

// Unwrap returns a client.ChangeStreamWatcher for backward compatibility.
// Fault-quarantine, node-drainer, and fault-remediation type-assert for this method.
func (w *KubernetesChangeStreamWatcher) Unwrap() client.ChangeStreamWatcher {
	slog.Debug("Unwrapping Kubernetes watcher to legacy interface", "clientName", w.clientName)
	return &kubernetesLegacyWatcher{parent: w}
}

// kubernetesLegacyWatcher adapts the Kubernetes watcher to the legacy client.ChangeStreamWatcher interface.
type kubernetesLegacyWatcher struct {
	parent *KubernetesChangeStreamWatcher
}

func (lw *kubernetesLegacyWatcher) Start(ctx context.Context) {
	lw.parent.Start(ctx)
}

func (lw *kubernetesLegacyWatcher) Events() <-chan client.Event {
	return lw.parent.oldEventChan
}

func (lw *kubernetesLegacyWatcher) MarkProcessed(ctx context.Context, token []byte) error {
	return lw.parent.MarkProcessed(ctx, token)
}

func (lw *kubernetesLegacyWatcher) Close(ctx context.Context) error {
	return lw.parent.Close(ctx)
}

// kubernetesEvent implements client.Event by wrapping a typed HealthEventResourceCRD.
type kubernetesEvent struct {
	cr *model.HealthEventResourceCRD
}

func (e *kubernetesEvent) GetDocumentID() (string, error) {
	return e.cr.Name, nil
}

func (e *kubernetesEvent) GetRecordUUID() (string, error) {
	return e.cr.Name, nil
}

func (e *kubernetesEvent) GetNodeName() (string, error) {
	if label, ok := e.cr.Labels[labelNodeName]; ok && label != "" {
		return label, nil
	}

	if e.cr.Spec != nil && e.cr.Spec.NodeName != "" {
		return e.cr.Spec.NodeName, nil
	}

	slog.Warn("nodeName not found in HealthEventResource", "crName", e.cr.Name)

	return "", fmt.Errorf("nodeName not found in HealthEventResource")
}

func (e *kubernetesEvent) GetResumeToken() []byte {
	return []byte(e.cr.ResourceVersion)
}

// UnmarshalDocument populates a model.HealthEventWithStatus directly from the typed CR.
func (e *kubernetesEvent) UnmarshalDocument(v interface{}) error {
	target, ok := v.(*model.HealthEventWithStatus)
	if !ok {
		return fmt.Errorf("unsupported target type for UnmarshalDocument: %T", v)
	}

	createdAt := e.cr.CreationTimestamp.Time
	if ann := e.cr.GetAnnotations(); ann != nil {
		if ts, ok := ann[originalHealthEventTimestamp]; ok {
			if t, err := parseTime(ts); err == nil {
				createdAt = t
			}
		}
	}

	target.HealthEvent = e.cr.Spec
	target.HealthEventStatus = e.cr.Status
	target.CreatedAt = createdAt

	slog.Debug("UnmarshalDocument completed", "crName", e.cr.Name, "createdAt", createdAt)

	return nil
}

var _ datastore.ChangeStreamWatcher = (*KubernetesChangeStreamWatcher)(nil)
var _ client.ChangeStreamWatcher = (*kubernetesLegacyWatcher)(nil)
var _ client.Event = (*kubernetesEvent)(nil)
