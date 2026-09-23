# Pallas 0.34 -> 1.1.1 (geofflittle/pallas, feat/dijkstra-era-accessors) migration notes

Local, uncommitted-upstream migration to unblock Musashi/Dijkstra testing for
scooper-v2. Pulls in pallas PRs #799 (merged), #800, #801 (open, unmerged) from
https://github.com/geofflittle/pallas, branch `feat/dijkstra-era-accessors`.

Goal: get `acropolis_common`, `acropolis_codec`, and the modules scooper-v2
depends on (block_unpacker, custom_indexer, genesis_bootstrapper,
mithril_snapshot_fetcher, peer_network_interface) compiling and behaviorally
correct against the new pallas. NOT touching modules scooper-v2 doesn't use.

## Changes

### acropolis (this clone)

- `common/src/types.rs`: `Era::try_from(u8)` — treat era >=6 as Conway instead
  of bailing on 7 (the earlier Musashi-specific workaround, kept; unrelated to
  the rest of this migration).
- `codec/src/utils.rs`: `Relay`, `PoolMetadata`, `Anchor`, `GovActionId` fields
  that were `Nullable<T>` in old pallas are now plain `Option<T>` — rewrote
  `map_relay`, `map_nullable_anchor`, `map_nullable_gov_action_id` accordingly;
  deleted the now-unused generic `map_nullable`/`map_nullable_result` helpers.
- `codec/src/certs.rs`, `codec/src/governance.rs`: same Option-not-Nullable
  fix at each call site (`pool_metadata`, `guardrail_script`, `script_hash`,
  proposal/action `previous_action_id`).
- `codec/src/script.rs`: `MultiEraRedeemer::tag()` now returns the era-neutral
  `MultiEraRedeemerTag` (was `conway::RedeemerTag`) — `map_redeemer_tag` now
  takes that type and returns `Result` (errors on Dijkstra's new `Guarding`
  purpose, which acropolis_common::RedeemerTag has no variant for).
- `codec/src/tx.rs`, `codec/src/utxo.rs`: `MultiEraTx::native_scripts()` and
  `MultiEraOutput::script_ref()` now return era-neutral `MultiEraNativeScript`/
  `MultiEraScriptRef` (PR #801's "four widened accessors"). Dijkstra native
  scripts and Dijkstra reference scripts (incl. PlutusV4) have no
  acropolis_common representation yet and are skipped — nothing in this
  migration's scope needs them (sundae-v4's Musashi deployment is PlutusV3).
  `original_hash()` → `.hash()`; `conway::MintedDatumOption` →
  `conway::DatumOption`; `conway::PseudoScript` → `conway::ScriptRef`.
- `codec/src/witness.rs`: `ScriptNOfK`'s threshold is now `i64` (ledger CDDL
  is signed), acropolis_common still wants `u32` — clamp to `u32::MAX` on
  overflow/negative rather than panicking on untrusted chain data.
- `modules/genesis_bootstrapper`: `pallas::ledger::configs` moved to
  `pallas::interop::hardano::configs`, gated behind the `hardano` feature
  (added to this crate's `pallas` dependency).
- `modules/mithril_snapshot_fetcher`: `pallas::storage::hardano` moved to
  `pallas::interop::hardano::storage` (aliased back to `hardano` on import so
  the rest of the file is unchanged). Everything else in this file (the
  `[u8]`-unsized, `Point::slot_or_default` type-inference errors) was
  cascading fallout from that one bad import — no changes needed once fixed.

### The pallas 0.34 → 1.1.1 jump itself (biggest single unlock)

`plutus-parser` (crates.io, independent of this migration) re-exports
`pallas_primitives::PlutusData` as its own `PlutusData`. Its default feature
pins pallas-primitives 0.34 from crates.io — a **second, separate copy** of
pallas-primitives from the git fork everything else uses, so `PlutusData:
AsPlutus`/`minicbor::Encode`/`Decode` all failed to resolve (two structurally
identical but distinct Rust types). plutus-parser already ships a `pallas-v1`
feature for exactly this (pinned to `pallas-primitives ^1.0.0-alpha.3`,
`minicbor 0.26`) — switched scooper-v2's `plutus-parser` dep to
`default-features = false, features = ["derive", "pallas-v1"]`, then added
`[patch.crates-io] pallas-primitives = { git = ..., branch = ... }` so that
`pallas-v1`'s crates.io pallas-primitives resolves to the same fork instead of
a third copy. Also had to bump scooper-v2's own direct `minicbor` dependency
from 0.25 to 0.26 to match pallas-codec's (the same duplicate-crate problem,
one layer down). This one combination resolved ~55 of the ~213 total errors.

### scooper-v2 itself

- Same `Nullable`→`Option`, `NonEmptyKeyValuePairs`→`BTreeMap`,
  `MultiEraRedeemerTag`/`MultiEraScriptRef`/`MultiEraNativeScript` widened-
  accessor, and `Pseudo*`→real-name fixes as acropolis, applied throughout
  `bootstrap.rs`, `cardano_types.rs`, `sundaev4/{tx_builder,indexer,butane,
  evaluator,script_context,scoop_tests}.rs`, `sundaev3/indexer.rs`.
- **KeepRaw wrapping is now pervasive**: `TransactionOutput::PostAlonzo`,
  `DatumOption::Data`, `Tx.transaction_body`/`transaction_witness_set`,
  `ScriptContext.redeemer`, `AuxiliaryData` all moved from bare values to
  `KeepRaw<'_, T>` (round-trip-preserving CBOR wrapper). Building fresh values
  uses `.into()` (produces an empty `raw` — confirmed safe: `KeepRaw::Encode`
  falls back to re-encoding `inner` whenever `raw_cbor()` is empty, so this
  doesn't corrupt output, verified by the full test suite passing including
  UPLC script-evaluation tests). Reading existing values uses
  `Option::as_deref()` to match through the wrapper.
- `conway::TransactionBody`/`conway::Tx` are no longer generic over the output
  type (`TransactionBody<'a, O>` → `TransactionBody<'a>`, hardcoded to
  `conway::TransactionOutput<'a>` internally) — `MultiPoolBuildResult.tx_body`
  changed from `MintedTransactionBody<TransactionOutput>` to
  `MintedTransactionBody<'static>` (safe: it's built from owned components via
  `.into()`, nothing genuinely borrows a short-lived buffer).
  `DeployedScript`/`ButaneRuntime`/`DepositPieces` in `butane.rs` needed the
  same `<'static>` treatment, but those decode from real byte buffers
  (deployment artifact CBOR) — used `Box::leak` on the owned `Vec<u8>` before
  decoding, since `ButaneRuntime` is loaded once at startup and held for the
  process lifetime (confirmed via grep: only referenced from `scooper.rs`'s
  long-lived `Scooper` struct, never round-tripped through a shorter-lived
  buffer).
- `pallas_primitives::babbage::MintedPostAlonzoTransactionOutput` (used
  throughout to build outputs) was **always the wrong type** even before this
  migration in spirit — it parameterizes with `alonzo::Value`/
  `babbage::ScriptRef`, not `conway::Value`/`conway::ScriptRef`. New pallas
  stopped accepting it as a stand-in; every construction site now uses
  `conway::PostAlonzoTransactionOutput` directly.
- `TxValidationError` (pallas-network) changed from a raw-bytes newtype to a
  real enum (`ByronTxValidationError`/`ShelleyTxValidationError`/`Plutus`) —
  `mempool.rs`'s rejection-reason formatting switched from hex-dumping
  `reason.0` to `format!("{reason:?}")`.
- Result: **233 passed, 0 failed, 3 ignored** — full existing suite green,
  including UPLC script-evaluation tests, so the KeepRaw/Value/ScriptRef
  migration is behaviorally correct, not just compiling.

## Live-verification findings on Musashi (post-migration)

Once the above got scooper-v2 compiling and chain-syncing cleanly, running it
live against a real Musashi node (Dijkstra era) surfaced two further bugs,
both **outside** the ledger-primitive/CBOR decode surface the
`feat/dijkstra-era-accessors` branch actually covers.

### 1. `convert_script_ref` silently dropped every Dijkstra reference script (FIXED)

`src/cardano_types.rs`'s `convert_script_ref` only handled
`MultiEraScriptRef::Conway`, returning `None` for anything else — including
`MultiEraScriptRef::Dijkstra`. Since **every** UTxO on Musashi is Dijkstra-era,
this meant scooper-v2's UPLC `ScriptStore` was permanently empty regardless of
how it was bootstrapped: chain-sync correctly spotted and persisted all the
module-script reference UTxOs (confirmed directly via sqlite — the `ref`-typed
rows were all there, right addresses, right slots), but every single one
decoded with `script_ref: None`, so eval always failed with "script ... not
found in store".

Fixed by extending `convert_script_ref` to also handle
`MultiEraScriptRef::Dijkstra`, mapping `NativeScript`/`PlutusV1Script`/
`PlutusV2Script`/`PlutusV3Script` the same way as Conway (Dijkstra's
`PlutusV4Script` and its distinct `NativeScript` type have no local
representation yet — same "unsupported, return None" precedent as the
existing Conway-only PlutusV4 gap). After this fix scooper-v2 correctly
evaluates and builds a fully valid scoop transaction (verified: correct swap
math, budgets well within limits — mem 20%, steps 9.8%, size 13.7%).

### 2. pallas-network's LocalTxSubmission has no Dijkstra support (PARTIALLY FIXED locally, not upstream)

Submitting the built tx via the local node's `LocalTxSubmission` mini-protocol
(`src/mempool.rs`'s `submit_via_node`, via `pallas_network`) failed on
`Error { err: UnknownVariant(7), pos: Some(4), msg: "" }` — for **every**
submission attempt, accept or reject.

Root cause: `pallas-network/src/miniprotocols/localtxsubmission/protocol.rs`'s
`ShelleyBasedEra` enum (used to tag which ledger era a `Reject` response's
`TxValidationError` came from) stops at `Conway = 6`:

```rust
#[cbor(index_only)]
pub enum ShelleyBasedEra { Shelley=1, Allegra=2, Mary=3, Alonzo=4, Babbage=5, Conway=6 }
```

A real Dijkstra cardano-node tags its era as index **7** (confirmed
independently: submitting the same tx directly via
`cardano-cli dijkstra transaction submit` against the same node produces a
Haskell error whose top constructor reads
`ShelleyTxValidationError ShelleyBasedEraDijkstra (...)`). `TxValidationError`'s
hand-written `Decode` impl reads `era` before `error` on the wire
(`codec.rs`: `d.array()?; d.array()?; let era = ...; let error = ...;`), so
this is the very first thing decoded in any `RejectTx` payload — hence the
crash at byte offset 4, before any of the actual failure-reason content is
ever touched. `pallas-hardano`'s `haskell_error.rs` has a second, JSON-only
mirror of the same enum with the same gap.

**Local fix applied** (`../pallas-patched`, a clone of
`geofflittle/pallas@feat/dijkstra-era-accessors`, patched in via
`[patch."https://github.com/geofflittle/pallas.git"]` in scooper-v2's
`Cargo.toml`, pointing all `pallas-*` crates scooper-v2 uses — plus
`pallas-hardano`, pulled in transitively — at local paths so there's one
consistent source): added `#[n(7)] Dijkstra` to both `ShelleyBasedEra` enums.
This is verified correct and effective — after the fix, decoding progresses
past the era tag; a live rejected submission now produces a **new, different**
error instead of `UnknownVariant(7)`.

**Not fixed — deliberately out of scope for now**: `ApplyTxError`'s payload
(`Vec<ConwayLedgerFailure>`, decoded immediately after `era`) is a large,
Conway-shaped enum tree (`ConwayLedgerFailure` → `ConwayUtxoWPredFailure` →
`UtxoFailure` → `BadInputsUTxO` etc.). Dijkstra's actual failure ADT has
genuinely different shapes here too — cardano-cli's own error text shows new
top-level wrapper constructors (`DijkstraUtxowFailure`, `DijkstraApplyTxError`)
around what look like the same inner substructures. Post-era-tag-fix, a live
Reject now fails with `Error { err: TypeMismatch(U8), pos: Some(11), msg:
"expected bytes (definite length)" }` inside this nested decode — confirming
the shape genuinely differs, not just the era tag. Modeling this correctly
would need either the raw CBOR bytes of a real Dijkstra rejection (not
captured — pallas fails before we get application-level access to them) or
cardano-ledger's actual Dijkstra CDDL/Haskell source for the new failure
constructors' CBOR tag numbers, neither of which is in hand. Until this is
done, scooper-v2 can only cleanly observe a submission's outcome via the local
node when the node **accepts** the tx (`Message::AcceptTx` has no payload, so
none of this decode path is touched) — any `Reject` response still fails to
parse past the failure-detail payload.

**Practical implication**: getting a scoop actually confirmed on-chain via
scooper-v2's own autonomous local-node submission requires the submitted tx to
be accepted on the first try (no partial-decode fallback for rejects yet) —
which in turn requires scooper-v2's wallet-UTxO tracking to only offer
genuinely-unspent inputs. See below.

### 3. Wallet-UTxO spent-tracking misses transactions that live only in Leios input blocks (ROOT-CAUSED — architectural gap, not a scooper-v2/acropolis bug)

Independently of the above: after a full clean genesis-to-tip resync, 27 of
29 UTxOs scooper-v2 believes it owns are already spent on-chain (verified
directly via `cardano-cli query utxo`, which returns `{}` for each). This
causes scooper-v2 to build transactions with phantom inputs that the ledger
rejects with `BadInputsUTxO`.

Ruled out: `handle_rollback` never fires during the resync (zero "v4: rolling
back to" log lines), so it isn't a rollback/replay gap. `tx.inputs()` returns
non-empty results for transactions generally (spot-checked many).

Root cause, confirmed via db-sync + blockfrost-backend-ryo cross-checks: every
single one of the 27 stale UTxOs was spent by a transaction that lives inside
a **Leios ranking block** — an on-wire block that is 73-74 bytes but whose
`tx_count` (per blockfrost) is in the hundreds or thousands:

| slot | on-wire block size | tx_count |
|---|---|---|
| 679456 | 73 bytes | 2341 |
| 679831 | 73 bytes | 2043 |
| 715076 | 74 bytes | 2140 |
| 771941 | 74 bytes | 850 |
| 837117 | 74 bytes | 2777 |
| 1002797 | 73 bytes | 2340 |

For comparison, a normal block scooper-v2 *does* process correctly (slot
836851) is 87,641 bytes for 436 txs — an ordinary ~200 bytes/tx. A 74-byte
block cannot physically contain thousands of real transactions; Leios splits
block production into a tiny ranking block (RB) that just certifies/
references a batch of transactions, with the actual transaction bodies
diffused separately via **input blocks (IBs)** over a different mechanism
than classic Praos block bodies.

acropolis (via the pallas ChainSync/BlockFetch client it uses) only
implements the traditional single-layer block-fetch protocol. It receives and
successfully decodes the 74-byte ranking block — nothing crashes or errors,
which is why this was hard to spot — but has **no protocol support for
fetching the input blocks it references**, so any transaction that lives only
in an IB (evidently a large fraction of this devnet's background/load-test
traffic, which happened to also consume several of scooper-v2's wallet's
chained change outputs along the way) is completely invisible to it.

This is **not** a bug in `acropolis_module_peer_network_interface`'s
block-flow/chain-state logic (`chain_state.rs`'s peer-switching, rollback, and
publish-queue logic all look correct on inspection) — it's a missing Leios
input-block diffusion mini-protocol, a materially larger and separate body of
work from the Dijkstra-era CBOR/ledger-type migration covered above. Neither
`geofflittle/pallas` nor upstream `txpipe/pallas` implements it as of this
writing.

## End-to-end verification: a real scoop, confirmed on-chain

With both live-verification fixes above in place (Dijkstra script-ref decode,
and the `ShelleyBasedEra`/era-tag fix in `../pallas-patched`), scooper-v2
correctly builds a valid scoop transaction for the test order — but the two
Leios-invisible-block issues (wallet-UTxO tracking in section 3, and the same
gap independently affecting the tracked *pool* input — its one-and-only
update tx also landed in a 74-byte/2777-tx ranking block) left scooper-v2
trying to spend inputs the ledger considers already gone.

**One-off manual correction** (not a code fix — see caveat below): copied
`scooper-v2`'s sqlite DB out of the running container, corrected it directly
from ground truth gathered via db-sync + `cardano-cli query utxo`:
- Marked the 27 stale `wallet`-typed rows' `spent_slot`/`spent_tx_id` (real
  values, looked up via db-sync's `tx_in` table) so `load()`'s existing
  `WHERE spent_slot IS NULL` filter correctly excludes them.
- Marked the stale `pool`-typed row (`7900572d...#0`) spent the same way, and
  inserted a new `pool` row for the pool's real current UTxO
  (`d38f1900...#0`, address/value/datum read directly via `cardano-cli query
  utxo --address`), encoded as a `conway::TransactionOutput::PostAlonzo` via
  a throwaway test using scooper-v2's own pallas-derived types (to guarantee
  the bytes are exactly what `load()`'s `MultiEraOutput::decode` expects —
  verified with a round-trip decode assertion before touching the real DB).

Copied the corrected DB back into the container and restarted. Result:
scooper-v2 built a scoop tx against the *true* current pool reserves
(1,001,000 / 999,004 → dx=1000, dy=994, correctly reflecting the price shift
from the earlier invisible swap), submitted it through the local node, and it
was **accepted and confirmed on-chain** — independently verified via
`cardano-cli query utxo` (order UTxO gone, pool now holds 1,002,000 / 998,010,
exactly matching the computed swap) and via blockfrost/db-sync (confirming tx
`2105c15b...` at slot 1013827, in a *normal* 2368-byte single-tx block — not
a Leios ranking block, so ordinary chain-sync could see it). scooper-v2's own
`/health` endpoint then independently corroborated this on its own, without
any further help: `orders_pending` dropped to 0, and `recent_scoops`/
`scooper_totals` recorded the scoop under its own keyhash.

**Caveat**: this DB correction was a one-off, out-of-band workaround for
*this specific* pool/wallet state, not a fix to scooper-v2 or acropolis. A
fresh resync (or any future pool update tx that also lands in a Leios input
block) will reproduce the same staleness — there is no general fix short of
implementing Leios input-block diffusion in pallas-network.

