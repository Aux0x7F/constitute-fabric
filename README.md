# constitute-fabric

Rust host-fabric composition boundary for Constitution.

`constitute-fabric` reduces host-fabric member contributions, lifecycle-plan
posture, and association handoff references into protocol-validated
`hostFabric.fulfillment.plan` records. It does not start processes, route
traffic, store bytes, bind media, or own service semantics.
It also reduces multi-gateway contract target registry posture from member
contribution outputs, so gateway, service-edge, platform-adapter, runtime, and
domain-service candidates are selected as target fulfillments rather than
global config or product branches.

## Boundary

- Owns: host composition reduction, required role coverage, missing role
  posture, aggregate lifecycle-plan posture, and fulfillment-plan projection.
- Consumes: protocol host-fabric member contributions, lifecycle-plan posture,
  and association handoff refs.
- Emits: protocol `hostFabric.fulfillment.plan` records and local reduction
  summaries for tests and callers; for target-registry reduction, protocol
  `contract.target.registry.posture` records.
- Does not own: lifecycle semantics, OS execution, service-manager operations,
  gateway routing, storage, media transport, or product UI policy.

## Local Checks

```bash
cargo test
```
