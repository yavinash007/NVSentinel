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
	"log/slog"

	"github.com/nvidia/nvsentinel/store-client/pkg/client"
	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"

	"k8s.io/client-go/dynamic"
)

// KubernetesChangeStreamWatcher implements both datastore.ChangeStreamWatcher and
// provides Unwrap() to return a client.ChangeStreamWatcher. This dual-interface
// support is required because fault-quarantine, node-drainer, and fault-remediation
// unwrap the abstract watcher to access the legacy client.ChangeStreamWatcher.Events() channel.
//
// Under the hood, this uses a Kubernetes watch (informer) on HealthEventResource CRs.
// Watch events (ADDED, MODIFIED) are translated into the same event format that
// MongoDB change streams and PostgreSQL changelog polling produce.
type KubernetesChangeStreamWatcher struct {
	dynamicClient dynamic.Interface
	namespace     string
	clientName    string

	eventChan     chan datastore.EventWithToken
	oldEventChan  chan client.Event
	stopCh        chan struct{}
}

// NewKubernetesChangeStreamWatcher creates a new Kubernetes change stream watcher.
func NewKubernetesChangeStreamWatcher(
	dynamicClient dynamic.Interface, namespace string, clientName string,
) *KubernetesChangeStreamWatcher {
	return &KubernetesChangeStreamWatcher{
		dynamicClient: dynamicClient,
		namespace:     namespace,
		clientName:    clientName,
		eventChan:     make(chan datastore.EventWithToken),
		oldEventChan:  make(chan client.Event),
		stopCh:        make(chan struct{}),
	}
}

// Events returns the abstract event channel (datastore.ChangeStreamWatcher interface).
func (w *KubernetesChangeStreamWatcher) Events() <-chan datastore.EventWithToken {
	return w.eventChan
}

// Start begins watching HealthEventResource CRs via the Kubernetes API.
func (w *KubernetesChangeStreamWatcher) Start(ctx context.Context) {
	slog.Info("Kubernetes change stream watcher started (stub)", "clientName", w.clientName)
	// TODO: Implement K8s watch/informer that pushes events to eventChan and oldEventChan
	<-ctx.Done()
}

// MarkProcessed is a no-op for Kubernetes. K8s watches are position-based via
// resourceVersion, which is tracked automatically by the informer.
func (w *KubernetesChangeStreamWatcher) MarkProcessed(_ context.Context, _ []byte) error {
	return nil
}

// Close stops the watcher and closes channels.
func (w *KubernetesChangeStreamWatcher) Close(_ context.Context) error {
	close(w.stopCh)

	return nil
}

// Unwrap returns a client.ChangeStreamWatcher for backward compatibility.
// Fault-quarantine, node-drainer, and fault-remediation type-assert for this method.
func (w *KubernetesChangeStreamWatcher) Unwrap() client.ChangeStreamWatcher {
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

var _ datastore.ChangeStreamWatcher = (*KubernetesChangeStreamWatcher)(nil)
var _ client.ChangeStreamWatcher = (*kubernetesLegacyWatcher)(nil)
