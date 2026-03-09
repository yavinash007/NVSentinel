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

	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"
	"github.com/nvidia/nvsentinel/store-client/pkg/watcher"
)

// KubernetesWatcherFactory implements WatcherFactory for the Kubernetes CRD provider.
type KubernetesWatcherFactory struct{}

// NewKubernetesWatcherFactory creates a new Kubernetes watcher factory.
func NewKubernetesWatcherFactory() watcher.WatcherFactory {
	return &KubernetesWatcherFactory{}
}

// CreateChangeStreamWatcher creates a Kubernetes change stream watcher backed by the
// Kubernetes Watch API on HealthEventResource CRDs.
func (f *KubernetesWatcherFactory) CreateChangeStreamWatcher(
	ctx context.Context,
	ds datastore.DataStore,
	config watcher.WatcherConfig,
) (datastore.ChangeStreamWatcher, error) {
	k8sStore, ok := ds.(*KubernetesDataStore)
	if !ok {
		return nil, fmt.Errorf("expected Kubernetes datastore, got %T", ds)
	}

	clientName := "watcher-factory"
	if config.Options != nil {
		if name, ok := config.Options["ClientName"].(string); ok && name != "" {
			clientName = name
		}
	}

	slog.Info("Creating Kubernetes change stream watcher",
		"clientName", clientName,
		"namespace", k8sStore.namespace)

	return NewKubernetesChangeStreamWatcher(
		k8sStore.k8sClient, k8sStore.namespace, clientName,
	), nil
}

// SupportedProvider returns the provider this factory supports.
func (f *KubernetesWatcherFactory) SupportedProvider() datastore.DataStoreProvider {
	return datastore.ProviderKubernetes
}

func init() {
	watcher.RegisterWatcherFactory(datastore.ProviderKubernetes, NewKubernetesWatcherFactory())
}
