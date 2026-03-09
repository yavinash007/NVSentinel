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
	"os"

	"github.com/nvidia/nvsentinel/data-models/pkg/model"
	"github.com/nvidia/nvsentinel/store-client/pkg/client"
	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// KubernetesDataStore implements the DataStore interface using Kubernetes CRs as the backing store.
// HealthEvent data is stored as HealthEventResource custom resources in the cluster's etcd
// via the Kubernetes API server, eliminating external database dependencies.
type KubernetesDataStore struct {
	k8sClient        crclient.WithWatch
	namespace        string
	healthEventStore datastore.HealthEventStore
	databaseClient   *KubernetesDatabaseClient
}

// NewKubernetesStore creates a new Kubernetes datastore backed by controller-runtime's typed client.
func NewKubernetesStore(ctx context.Context, config datastore.DataStoreConfig) (datastore.DataStore, error) {
	namespace := config.Options["namespace"]
	if namespace == "" {
		namespace = "nvsentinel"
	}

	restConfig, err := buildRESTConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to build Kubernetes REST config: %w", err)
	}

	scheme := runtime.NewScheme()
	if err := model.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("failed to register HealthEventResource types: %w", err)
	}

	k8sClient, err := crclient.NewWithWatch(restConfig, crclient.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("failed to create controller-runtime client: %w", err)
	}

	store := &KubernetesDataStore{
		k8sClient: k8sClient,
		namespace: namespace,
	}

	store.healthEventStore = NewKubernetesHealthEventStore(k8sClient, namespace)
	store.databaseClient = NewKubernetesDatabaseClient(k8sClient, namespace)

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

// Ping tests the Kubernetes API server connection by performing a lightweight List.
func (k *KubernetesDataStore) Ping(ctx context.Context) error {
	slog.Debug("Pinging Kubernetes API server", "namespace", k.namespace)

	list := &model.HealthEventResourceCRDList{}
	if err := k.k8sClient.List(ctx, list, crclient.InNamespace(k.namespace), crclient.Limit(1)); err != nil {
		slog.Error("Kubernetes API server ping failed", "error", err)
		return datastore.NewConnectionError(datastore.ProviderKubernetes, "failed to ping Kubernetes API server", err)
	}

	slog.Debug("Kubernetes API server ping succeeded")

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
func (k *KubernetesDataStore) CreateChangeStreamWatcher(
	ctx context.Context, clientName string, pipeline interface{},
) (datastore.ChangeStreamWatcher, error) {
	slog.Info("Creating change stream watcher", "clientName", clientName, "namespace", k.namespace)

	watcher := NewKubernetesChangeStreamWatcher(k.k8sClient, k.namespace, clientName)

	return watcher, nil
}

// NewChangeStreamWatcher creates a watcher from a generic config map.
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

// buildRESTConfig builds a Kubernetes REST config.
// Priority: in-cluster config > ~/.kube/config (for local dev) > explicit DATASTORE_HOST.
func buildRESTConfig(config datastore.DataStoreConfig) (*rest.Config, error) {
	restConfig, err := rest.InClusterConfig()
	if err == nil {
		slog.Info("Using in-cluster Kubernetes config")
		return restConfig, nil
	}

	slog.Info("Not running in-cluster, falling back to kubeconfig", "inClusterErr", err)

	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	configOverrides := &clientcmd.ConfigOverrides{}
	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)

	restConfig, err = kubeConfig.ClientConfig()
	if err == nil {
		slog.Info("Using kubeconfig")
		return restConfig, nil
	}

	explicitHost := os.Getenv("DATASTORE_HOST")
	if explicitHost != "" {
		slog.Info("Using explicit Kubernetes API server host from DATASTORE_HOST", "host", explicitHost)
		return &rest.Config{Host: explicitHost}, nil
	}

	return nil, fmt.Errorf("failed to build Kubernetes REST config: no in-cluster config, kubeconfig, or DATASTORE_HOST available")
}

var _ datastore.DataStore = (*KubernetesDataStore)(nil)
