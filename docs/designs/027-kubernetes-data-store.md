# ADR-027: Datastore Kubernetes Custom Resource (CR) for HealthEvent

Author: Avinash Reddy Yeddula, Igor Velichkovich

## Context

NVSentinel currently persists and watches health events using MongoDB through a datastore client. While MongoDB provides rich query capabilities and watch semantics, reliance on an external database makes NVSentinel harder to deploy in Kubernetes-native or constrained environments and prevents exposing health events through standard Kubernetes APIs.

A broader datastore abstraction and multi-backend strategy is defined in the NVSentinel Datastore Abstraction Design document. This ADR narrows the scope to the Kubernetes Custom Resource (CR) datastore option and formalizes the design of the Kubernetes HealthEvent API and its operational constraints.

## Decision

We will introduce a Kubernetes-native HealthEvent API and implement a Kubernetes Custom Resource–backed datastore for NVSentinel that supports core event ingestion, status updates, and watch-driven remediation workflows.

In the initial phase, Kubernetes CRs are considered suitable only for NVSentinel’s core, watch-oriented workflows and not for analytics or historical querying.

## Implementation

### API Definition

- API Group: `healthevents.dgxc.nvidia.com`
- Kind: `HealthEvent`
- `spec`: contains immutable event data (from HealthEvent)
- `status`: tracks mutable workflow state (from HealthEventStatus)

### Directory Structure and CRD Generation

- Data Models: All new structs for the Kubernetes HealthEvent CRD (HealthEventSpec, HealthEventStatus, BehaviourOverrides, Entity, etc.) are added to `./data-models`.
- CRD Generation: `controller-gen` is used to generate the Kubernetes CRD YAML manifests from these Go structs.
- **Conversion APIs:** The conversion functions between internal NVSentinel models and CRD types are implemented in `pkg/conversion/healthevent_conversion.go` with corresponding unit tests in `pkg/conversion/healthevent_conversion_test.go`. These tests ensure that the mapping between internal models, protobufs, and CRD structs is correct and that there is no drift between the data models.
- Helm Chart Packaging: The generated CRD manifests are packaged in a new Helm chart at `./distros/kubernetes/nvsentinel/charts/kubernetes-store`.

### Operational Workflow

**Event Flow and CRD Integration:**

1. Health events are received by the `FaultRemediationReconciler` from the datastore change stream.
2. Each event is parsed and converted into a Kubernetes HealthEvent CR using the conversion APIs provided in the codebase. These APIs:
   - Convert between the internal NVSentinel model (`model.HealthEvent`, `model.HealthEventStatus`, etc.) and the CRD representation (`v1alpha1.HealthEventSpec`, `v1alpha1.HealthEventStatus`, etc.).
   - Handle nested types such as `OperationStatus`, `BehaviourOverrides`, and `Entity`.
   - Convert protobuf-based health events (`protos.HealthEvent`) into CRD spec and vice versa.
   - Map enumerations such as `Status`, `RecommendedAction`, and `ProcessingStrategy` between model, protobuf, and CRD types.
3. The reconciler creates or updates the CR in the Kubernetes API:
   - `spec` contains immutable event data.
   - `status` tracks mutable workflow state, such as node quarantine status, pod eviction progress, and remediation timestamps.
4. Operators or controllers can act on the CR data to determine remediation actions independently, providing flexibility and separation between event ingestion and execution.

### Integration with Existing Tooling and Processes

- The `FaultRemediationReconciler` continues to receive health events from the existing datastore change stream.
- Events are converted into HealthEvent CRs using the provided conversion APIs, enabling Kubernetes-native persistence of both immutable and mutable state.
- Existing remediation logic, log collection, and state management remain unchanged — they now operate in parallel with CR creation/update.
- No changes are required to the MongoDB-backed store; the Kubernetes store is an optional, pluggable backend.
- Other operators or controllers can consume the CRs for monitoring, remediation, or reporting in a decoupled manner.
- Standard Kubernetes mechanisms such as the `/status` subresource and annotations are leveraged to ensure minimal disruption and native observability.
- The Kubernetes CRD datastore is optional and can be disabled through the Helm chart configuration, allowing clusters to continue using the existing MongoDB-backed datastore if desired.

### CR Cleanup / Garbage Collection:
To prevent uncontrolled growth of HealthEvent CRs and reduce load on the Kubernetes API server, the Kubernetes store implementation includes:
- Optional TTL / age-based deletion – CRs older than a configured threshold can be automatically removed if still unresolved, preventing indefinite growth.
- Enforcement of configurable per-node or per-cluster CR limits – ensures the number of CRs remains manageable even during bursty failure periods or in large clusters.

## Rationale

- **Operational simplicity:** Using Kubernetes CRs leverages the existing control plane and standard Kubernetes mechanisms, eliminating the need for a separate database deployment and management.
- **Native observability and integration:** CRs can be watched, queried (to a limited extent), and annotated using Kubernetes-native APIs, making integration with controllers, dashboards, and CI/CD pipelines easier.
- **Developer experience and flexibility:** The CRD abstraction allows developers to work with Kubernetes-native resources while keeping the underlying datastore pluggable, enabling future enhancements or alternative datastore implementations.

## Consequences

### Positive

- Kubernetes-native storage: Events are persisted directly in the cluster, reducing external dependencies.
- Core workflows supported: Watch-oriented health event processing (quarantine, remediation, basic updates) works without modification.
- Independent remediation actions: Each operator or system can decide how to act on a health event based on the CR’s data, providing flexibility for different clusters, teams, or automation policies.
- Improved integration for automation: Other operators or controllers can consume CRs for monitoring, remediation, or reporting in a decoupled manner.

### Negative

- Limited querying capabilities: Historical analysis and complex queries are not feasible using only CRs.
- Control plane load: High-frequency or bursty events can increase the number of CR objects, potentially impacting the API server in large clusters.

### Mitigations

- Event deduplication and rate limiting: Only create CRs for unique unhealthy events or updates to existing events.
- Resource cleanup and garbage collection: Automatically remove CRs when events are remediated or when cluster/node limits are reached.
- Phased approach: Core watch-based workflows are enabled first; advanced features can be added later.

## Alternatives Considered

## Notes

- The initial phase only supports core, watch-driven workflows; analytics and historical queries are deferred.
- The system is designed to allow multiple operators or controllers to act independently on CR data.

## References

- NVSentinel Datastore Abstraction Design Document
- [K8s API Data store WIP PR](https://github.com/NVIDIA/NVSentinel/pull/640)
