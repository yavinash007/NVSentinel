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

	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"
)

func init() {
	slog.Info("Registering Kubernetes datastore provider")
	datastore.RegisterProvider(datastore.ProviderKubernetes, NewKubernetesDataStore)
}

// NewKubernetesDataStore creates a new Kubernetes datastore instance from configuration.
func NewKubernetesDataStore(ctx context.Context, config datastore.DataStoreConfig) (datastore.DataStore, error) {
	return NewKubernetesStore(ctx, config)
}
