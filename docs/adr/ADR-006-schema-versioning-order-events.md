# ADR-006: Schema versioning for the order event envelope

**Status:** Accepted

## Context

The `OrderPlaced` event envelope (`eventId`, `sku`, `quantity`, `customerId`, `timestamp`) had no
versioning of any kind. A field rename on the gateway side (e.g. `quantity` → `qty`) would
silently break the Scala consumer at the worst possible layer: not a decode failure with a clear
error, but a *successful* decode into the wrong shape (a missing required field fails loudly via
circe's `DecodingFailure`, but a rename that happens to collide with an unrelated existing field,
or a type change that circe can coerce, would not). There was no mechanism for the consumer to
know it was looking at a payload shape it didn't understand, and no documented rule for what
counts as a safe change to this envelope versus a breaking one.

## Options considered

1. **Full schema registry (e.g. Confluent Schema Registry, Avro/Protobuf with compatibility
   checks enforced at publish time)** — rejected as disproportionate for a 5-field JSON envelope
   on a single topic; it solves a problem (many producers/consumers, many schemas, automated
   compatibility enforcement at the broker) this system doesn't have.
2. **A `version` field on the envelope, checked by the consumer, with documented compatibility
   rules and no further tooling** (chosen) — the smallest mechanism that actually closes the
   gap: a consumer that doesn't recognize a version can say so explicitly instead of guessing.
3. **No versioning, rely on Pact contract tests alone to catch breaking changes before
   deployment** — this is what existed before. Rejected: Pact (`contracts/pacts/`) verifies the
   gateway and consumer agree on a shape *at the time the tests run*, but provides no protection
   once both sides are deployed and a future change to one side ships without the other —
   exactly the silent-mismatch scenario above. Versioning and contract testing are complementary,
   not substitutes: contract tests catch the mismatch in CI when both sides change together;
   the version field catches it in production if they don't.

## Decision

Option 2. `OrderPlaced` (Scala) and `OrderPlacedEvent` (C#) both gained a `version` field,
defaulting to `"1"` — the schema as it existed before this field existed. The gateway always
emits the current version (`"1"` today); it never needs to read or branch on it, since it's the
producer, not a consumer of its own messages.

The Scala consumer checks `order.version` against `RabbitConsumer.SupportedVersions` (currently
`Set("1")`) immediately after UUID validation, via the new `validateVersion`
(`processing-engine-scala/src/main/scala/com/polyglider/consumer/RabbitConsumer.scala`). An
unrecognized version raises `UnsupportedSchemaVersionException`, which — like a malformed
UUID — isn't given an explicit case in `ProcessingFailure.classify` (ADR-004); it falls through
the existing default-to-Permanent rule, since "I don't understand this payload's version" is
exactly the same shape of problem as "this UUID is malformed": a property of this specific
message that retrying cannot fix. The message is nacked straight to the DLX, no backoff retries
wasted, consistent with every other permanent failure. `DlqReprocessor` reuses the same
`validateUuids`/`validateVersion` chain, so a reprocessed DLQ message gets the identical check.

**Compatibility rules:**

- **Additive changes are backward compatible without a version bump.** A new optional field
  (handled the way `version` itself was introduced — absence treated as a documented default,
  via `OrderPlaced`'s custom circe `Decoder` using `c.getOrElse[String]("version")("1")` rather
  than relying on circe-generic's auto-derivation to guess at default-value semantics) doesn't
  require either side to change its version check.
- **Removals and renames require a version bump.** Anything that changes the *meaning* of an
  existing field name (a rename, a type change, repurposing a field) is breaking by definition —
  the old consumer would silently misinterpret the new shape, or vice versa. This requires: (a)
  the gateway starts emitting the new version, (b) the consumer's `SupportedVersions` is extended
  (not replaced) to include it, ideally with explicit decode/handling logic per version so old
  and new shapes can both be processed during the migration window, and (c) once no traffic uses
  the old version (observable via... see "Open questions" below), the old version is dropped.
- **Unrecognized versions are always a permanent failure**, never a transient one — there's no
  infrastructure condition that would make a consumer suddenly understand a version it doesn't
  have decode logic for, so retrying is never useful here.

## Consequences

- **Gained:** a future schema change that breaks compatibility now fails loudly and immediately
  (straight to DLX, milliseconds, same as any other permanent failure) instead of silently
  corrupting ledger data or crashing on a downstream `DecodingFailure` for an unrelated reason.
- **Gained:** the existing pre-#41 message shape (no `version` field at all — e.g.
  `tools/chaos/publish_chaos_message.py`'s hand-built payloads) continues to work unmodified,
  proving the additive-compatibility rule in practice rather than just asserting it.
- **Given up / open questions:** this ADR does not define *how* a future version bump's
  migration window ends in practice (when is it safe to stop supporting the old version?) —
  that depends on whatever observability exists at the time (e.g. a per-version counter on
  `polyglider_messages_processed_total`, not present today) and is deliberately left for that
  future change to decide, rather than speculatively building tooling for a migration that
  hasn't happened yet.
- **Given up:** this is a convention enforced by one consumer reading one field, not a registry
  that prevents an incompatible producer from publishing in the first place. A gateway change
  that breaks the envelope shape *without* bumping `version` defeats this entirely — the
  protection only works if both sides honor the convention, which Pact's contract tests
  (`OrderPlacedEventContractSpec.scala`, `OrderPlacedEventProviderTests.cs`) are what actually
  catch at development time, before the version field's runtime check would ever be needed.
