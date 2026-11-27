# Security boundary

<!-- Replace this section with the product's actual boundary before
implementing mutations: authorized roots/tenants, subjects, side effects and a
real reporting channel. -->

The product exposes read-only tools plus one write, `rate_result`, which stores
an orchestrator's rating in the service's private SQLite database through the
peer-UID-authorized Unix socket. Only `allowed_uids` (and the service user), or
exactly `rating_uids` when configured, may write; the ingest token is not
involved. It performs no other mutation, external SaaS access or credential handling. Describe the
actual authorized roots, subjects and side effects before implementing
mutations. Tool annotations and caller-supplied actor labels are not
authentication.

Do not publish secrets or raw user content in logs, snapshots or evidence.
Report security concerns privately to the repository owner; establish a real
reporting channel before public distribution.
