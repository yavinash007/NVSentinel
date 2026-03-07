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

	"github.com/nvidia/nvsentinel/store-client/pkg/client"
	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// KubernetesDataStore implements the DataStore interface using Kubernetes CRs as the backing store.
// HealthEvent data is stored as HealthEventResource custom resources in the cluster's etcd
// via the Kubernetes API server, eliminating external database dependencies.
type KubernetesDataStore struct {
	clientset        kubernetes.Interface
	dynamicClient    dynamic.Interface
	namespace        string
	healthEventStore datastore.HealthEventStore
	databaseClient   *KubernetesDatabaseClient
}

// NewKubernetesStore creates a new Kubernetes datastore.
func NewKubernetesStore(ctx context.Context, config datastore.DataStoreConfig) (datastore.DataStore, error) {
	namespace := config.Options["namespace"]
	if namespace == "" {
		namespace = "nvsentinel"
	}

	restConfig, err := buildRESTConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to build Kubernetes REST config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes clientset: %w", err)
	}

	dynClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes dynamic client: %w", err)
	}

	store := &KubernetesDataStore{
		clientset:     clientset,
		dynamicClient: dynClient,
		namespace:     namespace,
	}

	store.healthEventStore = NewKubernetesHealthEventStore(dynClient, namespace)
	store.databaseClient = NewKubernetesDatabaseClient(dynClient, namespace)

	slog.Info("Successfully created Kubernetes datastore", "namespace", namespace)

	return store, nil
}

// MaintenanceEventStore returns the maintenance event store.
// Not yet implemented for the Kubernetes provider.
func (k *KubernetesDataStore) MaintenanceEventStore() datastore.MaintenanceEventStore {
	return nil
}

// HealthEventStore returns the health event store.
func (k *KubernetesDataStore) HealthEventStore() datastore.HealthEventStore {
	return k.healthEventStore
}

// Ping tests the Kubernetes API server connection.
func (k *KubernetesDataStore) Ping(ctx context.Context) error {
	_, err := k.clientset.Discovery().ServerVersion()
	if err != nil {
		return datastore.NewConnectionError(datastore.ProviderKubernetes, "failed to ping Kubernetes API server", err)
	}

	return nil
}

// Close is a no-op for Kubernetes (no persistent connections to close).
func (k *KubernetesDataStore) Close(_ context.Context) error {
	return nil
}

// Provider returns the provider type.
func (k *KubernetesDataStore) Provider() datastore.DataStoreProvider {
	return datastore.ProviderKubernetes
}

// GetDatabaseClient returns the Kubernetes implementation of client.DatabaseClient.
// Required for backward compatibility with consumers that type-assert for this method
// (fault-quarantine, node-drainer, fault-remediation, health-events-analyzer).
func (k *KubernetesDataStore) GetDatabaseClient() client.DatabaseClient {
	return k.databaseClient
}

// CreateChangeStreamWatcher creates a Kubernetes watch-based watcher that implements
// the same change-stream semantics as MongoDB/PostgreSQL.
// The clientName and pipeline parameters are accepted for interface compatibility
// but pipeline filtering is handled differently in the Kubernetes provider.
func (k *KubernetesDataStore) CreateChangeStreamWatcher(
	ctx context.Context, clientName string, pipeline interface{},
) (datastore.ChangeStreamWatcher, error) {
	watcher := NewKubernetesChangeStreamWatcher(k.dynamicClient, k.namespace, clientName)

	return watcher, nil
}

// NewChangeStreamWatcher creates a watcher from a generic config map.
// Matches the interface used by the datastore abstraction layer.
func (k *KubernetesDataStore) NewChangeStreamWatcher(
	ctx context.Context, config interface{},
) (datastore.ChangeStreamWatcher, error) {
	configMap, ok := config.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unsupported config type: %T", config)
	}

	var clientName string
	if val, ok := configMap["ClientName"].(string); ok {
		clientName = val
	}

	if clientName == "" {
		return nil, fmt.Errorf("ClientName is required")
	}

	var pipeline interface{}
	if val, ok := configMap["Pipeline"]; ok {
		pipeline = val
	}

	return k.CreateChangeStreamWatcher(ctx, clientName, pipeline)
}

// buildRESTConfig builds a Kubernetes REST config from the datastore config.
// When running inside a cluster, uses in-cluster config.
// The config.Connection.Host can optionally override the API server URL.
func buildRESTConfig(config datastore.DataStoreConfig) (*rest.Config, error) {
	if config.Connection.Host != "" {
		return &rest.Config{
			Host: config.Connection.Host,
		}, nil
	}

	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in-cluster config (not running in a cluster?): %w", err)
	}

	return restConfig, nil
}

var _ datastore.DataStore = (*KubernetesDataStore)(nil)
