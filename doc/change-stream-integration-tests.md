# Change Stream Integration Tests

Here is a comprehensive list explaining each integration test added for testing change streams.

Tests are grouped by file. Within each file, tests are listed in the order they appear in
the file. Each test runs against one of two possibilities:

- **The local Docker Spanner emulator** - the default backend for most of these tests, no
  extra flags needed.
- **A real, multi-region Spanner instance** (`-Dspanner.test.real=true`, on classes annotated
  `@RealSpannerCompatible` that also override their connection/config to use it - currently
  `ChangeStreamCorrectContentIT`, `ChangeStreamFilterIT`, `ChangeStreamOrderingAndTransactionalIT`,
  `ChangeStreamValueCaptureTypeIT`, `ConcurrentKeysIT`, `DataTypesIT`, `ExcludeTtlDeletesFilterIT`,
  `InterleavedTableIT`, `MutableKeyRangeIT`, `PlacementMoveIT`, and `TransactionRecordCountIT`)
  - the preferred backend for `MUTABLE_KEY_RANGE` tests going forward; several key scenarios in
  `MutableKeyRangeIT` (the schema-change tests and the forced key-range-split tests) have been
  independently confirmed passing against it.

## `ChangeStreamCorrectContentIT`

Parameterized across all partition modes. Verifies the actual content of change-stream
records matches documented behavior for various row/column scenarios.

Confirmed passing against the local Docker emulator, all parameterized tests, both partition
modes. `@RealSpannerCompatible` with a connection/config override (`-Dspanner.test.real=true`).
Confirmed passing against real Spanner too, both partition modes, for every test except
`shouldCarryUnchangedColumnsThroughOnPartialUpdate` (self-skips there - see below).

**`shouldCarryAllPrimaryKeyColumnsInKeyStruct`**
Uses a composite primary key (two columns). Inserts two rows that share one key column but
differ on the other, then updates one of them. Confirms the Kafka record key includes both
key columns in the correct order, the two rows never collide onto the same key, and the
update is correctly scoped to only the matching row.

**`shouldCarryLastKnownValuesInBeforeOnDelete`**
Inserts a row, updates one of its columns, then deletes it. Confirms the delete's `before`
image reflects the updated value (not the original insert value), `after` is null, and a
tombstone follows.

**`shouldRoundTripNullColumnTransitions`**
Inserts a row with a null column, sets it to a value, then sets it back to null. Confirms
each transition is captured correctly, including that "value is null" is correctly
distinguished from "field never set" on the return-to-null step.

**`shouldCarryUnchangedColumnsThroughOnPartialUpdate`**
Inserts a row with several columns, then updates only one of them. Confirms `before` and
`after` both correctly carry through the untouched columns' values, showing a real
old-to-new transition only for the column that was actually touched. Passes against the
emulator. Self-skips against real Spanner, on both partition modes - see Known Issues #1
below for the full explanation.

**`shouldPickUpColumnAddedAfterStreamCreationWithoutReconfiguring`**
The parameterized (all-partition-mode) equivalent of `MutableKeyRangeIT`'s schema-change
tests above: inserts a row, adds a column mid-stream via DDL, inserts a second row using the
new column, and updates the first row's new column. Confirms records before and after the
schema change all look correct.

**`shouldDefaultTransactionTagAndSystemTransactionFlagForOrdinaryWrites`**
Performs a plain insert with no explicit transaction tag. Confirms the record's source
metadata shows an empty tag and a `false` system-transaction flag by default.

**`shouldSurfaceExplicitTransactionTag`**
Runs an insert inside a transaction carrying an explicit tag and confirms the tag surfaces on
the record. Parameterized across both partition modes. Self-skips (`Assumptions.assumeTrue`)
unless `-Dspanner.test.real=true` is set, since the local Docker emulator doesn't propagate
transaction tags through its change stream at all.

## `ChangeStreamFilterIT`

Tests for change-stream event filters (excluding specific operation types or specific
transactions from the stream), and confirms filtered-out changes still correctly affect what
later, unfiltered events report as the "before" state.

Parameterized across both partition modes; `@RealSpannerCompatible` with a connection/config
override (`-Dspanner.test.real=true`). Confirmed passing on both the emulator and real
Spanner, for both partition modes. Every table here has a single non-key column, and every
`UPDATE` always sets it, so none of these tests can trigger the before-image gap documented
elsewhere (that only shows up when an `UPDATE` leaves some non-key column out of its `SET`
clause).

**`shouldExcludeDeleteEventsAndTheirTombstones`**
With deletes excluded from the stream: inserts, updates, then deletes a row, and inserts an
unrelated second row. Confirms no delete or tombstone appears for the deleted row, while
everything else streams normally.

**`shouldReflectRealPriorStateOnUpdateAfterAnExcludedInsert`**
With inserts excluded from the stream: inserts a row (never streamed), then updates and
deletes it, plus inserts an untouched second row. Confirms no insert record appears, but the
update's `before` still correctly reflects the row's real original value even though its
insert was never seen, and the delete/tombstone still work normally.

**`shouldExcludeUpdateEventsButReflectRealStateOnSubsequentDelete`**
With updates excluded from the stream: inserts, updates (excluded), then deletes a row, plus
inserts an untouched second row. Confirms no update record appears, but the delete's `before`
correctly reflects the real (post-update) database state rather than a stale pre-update
value.

**`shouldNotRecordTransactionExplicitlyExcludedFromChangeStreams`**
Inserts a row, then runs an update inside a transaction explicitly marked to be excluded from
change streams, followed by a normal, visible update. Confirms the excluded transaction
produces no record at all, while the final visible update's `before` correctly reflects the
real state left behind by the excluded transaction.

## `ChangeStreamOrderingAndTransactionalIT`

Parameterized across both partition modes; each test creates and drops its own
partition-mode-suffixed table/change stream per invocation. Verifies cross-table transaction
correlation, strict ordering under rapid writes, and restart correctness.
`@RealSpannerCompatible` with a connection/config override (`-Dspanner.test.real=true`).

**`shouldCorrelateChangesAcrossTablesInSameTransaction`**
Seeds a row in table A, then runs one atomic transaction that updates table A and inserts
into table B. Confirms both changes from that shared transaction carry the same transaction
ID and commit timestamp, while the earlier, separate seed transaction has a different ID.
Passes on both the emulator and real Spanner, for both partition modes.

**`shouldPreserveStrictOrderAcrossManyRapidUpdatesToSameRow`**
Inserts a row, then issues eight rapid sequential updates to it. Confirms every resulting
record appears in exactly the order the updates were issued, with strictly increasing commit
timestamps throughout. Passes on both the emulator and real Spanner, for both partition modes.

**`shouldResumeWithoutDuplicatingOrLosingContentAcrossRestart`**
Inserts a row, confirms delivery, stops the connector, updates the row while it's down,
restarts, and confirms the missed update is delivered exactly once (not lost, not
duplicated) with correct before/after content. Then performs one more update after resuming
to confirm the connector keeps working normally afterward. Passes on the emulator for
`IMMUTABLE_KEY_RANGE`.

`MUTABLE_KEY_RANGE` self-skips on the emulator, and this test also fails against real
Spanner on both partition modes - see Known Issues #3 below for the full explanation.

## `ChangeStreamValueCaptureTypeIT`

Verifies the three non-default `value_capture_type` change-stream options behave as
documented, each inserting a row and updating only one of its columns.

**Runs against:** the local Docker emulator by default; `@RealSpannerCompatible` with a
connection/config override (`-Dspanner.test.real=true`).
Parameterized across both partition modes. All three tests pass on the emulator for both
`IMMUTABLE_KEY_RANGE` and `MUTABLE_KEY_RANGE`. Against real Spanner, two of the three tests
self-skip - see Known Issues #1 below for the full explanation and the per-`value_capture_type`
breakdown.

**`shouldCaptureFullNewRowWithNoNonKeyOldValues`** (`NEW_VALUES`)
Confirms `before` contains only the primary key (no old values), while `after` contains the
full row, including untouched columns. **Self-skips against real Spanner** - see Known
Issues #1 below.

**`shouldCaptureFullNewRowWithNoOldValues`** (`NEW_ROW`)
Confirms `before` contains no old column values (just the key), while `after` contains the
complete row.

**`shouldCaptureFullRowOnBothSides`** (`NEW_ROW_AND_OLD_VALUES`)
Confirms both `before` and `after` contain the complete row, with `before` showing the prior
value of the touched column and `after` showing its new value. **Self-skips against real
Spanner** - see Known Issues #1 below.

## `ConcurrentKeysIT`

**Currently `@Disabled`** (both partition modes) - see below.

**`shouldNotCrossContaminateStateBetweenInterleavedKeys`**
Parameterized across both partition modes. Inserts four different rows, then updates all four
in a deliberately interleaved order (not fully processing one key before starting the next),
to stress any per-row state tracking that might be indexed incorrectly. Confirms all eight
resulting events are captured and, critically, that each row's update correctly reflects that
row's own prior/new values - not a value that leaked in from a different row being processed
nearby.

`MUTABLE_KEY_RANGE` can't run against the emulator - see Known Issues #2 below. Against real
Spanner, both partition modes stream and reach the assertions, but both fail - see Known
Issues #1 below.

## `CrossPartitionSplitOrderingIT`

**Runs against:** the local Docker emulator only, and deliberately so - this test relies on
the emulator's own quirk of automatically re-splitting partitions on a timer, which real
Spanner doesn't do (real Spanner splits based on load, not a fixed schedule). The emulator's
timer-driven splitting is used on purpose here to get a churning partition topology "for free"
without needing to force a split manually.

**`shouldDeliverFollowUpWriteExactlyOnceAndInOrderAcrossBackgroundPartitionSplits`**
Parameterized across both partition modes, but only `IMMUTABLE_KEY_RANGE` actually runs;
`MUTABLE_KEY_RANGE` self-skips. Inserts a row, then waits 45 seconds - long enough for the
Spanner emulator's own timer-driven background partition splitting to run through several
generations of splits on its own, with no forced split needed - then updates that row.
Confirms exactly one insert and one update are delivered (no duplicates or drops from the
row's key range having moved across several partition generations), and that the update's
timestamp is strictly later than the insert's.

`MUTABLE_KEY_RANGE` self-skips - see Known Issues #2 below for the full explanation.

## `DataTypesIT`

Pre-existing file; `shouldRoundTripEdgeCaseValuesAcrossInsertUpdateDelete` is the newer of the
two tests. Both are now parameterized across both partition modes and `@RealSpannerCompatible`
with a connection/config override (`-Dspanner.test.real=true`).
Confirmed passing on both the emulator and real Spanner, for both partition modes, on both tests.

**`shouldStreamUpdatesToKafkaWithTheCorrectType`**
Inserts one row covering every supported column type (`BOOL`, `INT64`, `FLOAT32`, `FLOAT64`,
`TIMESTAMP`, `DATE`, `STRING`, `BYTES`, `NUMERIC`, `JSON`, an `ARRAY`, and a generated
`TOKENLIST` column). Confirms the resulting insert record's `after` struct carries each value
through with the correct type and value.

**`shouldRoundTripEdgeCaseValuesAcrossInsertUpdateDelete`**
Inserts a row with intentionally tricky values - an empty string, empty bytes, a negative
`NUMERIC`, a unicode/emoji string, and an empty array - then updates the row to flip the
empty string to `NULL`, set real bytes content, swap in a large positive `NUMERIC`, change
the unicode string, and populate the array, then deletes the row. Confirms empty values
round-trip as empty (not coerced to `NULL`) on insert, that the update's `before`/`after`
correctly show the edge-case-to-new-value transitions (including empty-string-to-`NULL`), and
that the delete's `before` reflects the post-update state with a trailing tombstone.

## `ExcludeTtlDeletesFilterIT`

Parameterized across both partition modes; `@RealSpannerCompatible` with a connection/config
override (`-Dspanner.test.real=true`). Passes on both the emulator and real Spanner, for both
partition modes - but this pass is likely inconclusive rather than a genuine confirmation of
the filter. See Known Issues #4 below for the full explanation.

**`shouldFilterOutTtlDeletesButStillDeliverUserIssuedDeletes`**
Inserts one row eligible for immediate TTL eviction and a second row with a far-future
expiration that's explicitly deleted by the user, on a change stream configured with
`exclude_ttl_deletes`. Confirms the user-issued delete and its tombstone still arrive
normally, while no delete or tombstone ever appears for the TTL-expired row.

## `InterleavedTableIT`

Parameterized across both partition modes; `@RealSpannerCompatible` with a connection/config
override (`-Dspanner.test.real=true`). Confirmed passing on both the emulator and real
Spanner, for both partition modes - unaffected by the before-image gap documented elsewhere
since it only issues an INSERT and a DELETE, no partial UPDATE.

**`shouldCaptureCascadingDeleteOfInterleavedChildRows`**
Inserts a parent row and a child row interleaved under it, in one transaction, then deletes
only the parent (never issuing any DML directly against the child), relying on
`ON DELETE CASCADE` to remove the child. Confirms both parent and child each produce insert,
delete, and tombstone events; that the parent and child inserts share one transaction ID
(proving the atomic multi-table insert is correlated); and that the parent's explicit delete
and the child's cascaded delete also share one transaction ID - confirming the
cascade-triggered child delete is correctly captured even though no direct DML touched the
child.

## `MutableKeyRangeIT`

Dedicated suite for behavior that's unique to `MUTABLE_KEY_RANGE` partition mode - sliding
window mechanics, connector-restart mechanics, and key-range-split mechanics - with no
`IMMUTABLE_KEY_RANGE` equivalent to test alongside.

**Runs against:** the local Docker emulator by default, which supports `MUTABLE_KEY_RANGE`
change streams directly; also `@RealSpannerCompatible` (`-Dspanner.test.real=true`). The two
forced-key-range-split tests below are the exception - they self-skip on the emulator
(`Assumptions.assumeTrue`) because it doesn't implement the `AddSplitPoints` admin RPC they
rely on (fails with `UNIMPLEMENTED`), so they only run against real Spanner. Every other test
passes on the emulator.

**`shouldStreamCrudEventsToKafka`**
Inserts a row, updates it, then deletes it. Confirms the four resulting events arrive in
order: an insert (`c`), an update (`u`), a delete (`d`), and a trailing tombstone.

**`shouldNotRepublishEventsAfterConnectorRestart`**
Inserts a row, confirms it's delivered, stops the connector, inserts a second row while it's
down, then restarts. Confirms the second row is delivered after restart (the first row may or
may not be redelivered, since delivery is at-least-once, not exactly-once).

**`shouldNotReplayAfterWindowElapses`**
Inserts a row, waits for the sliding window it falls in to fully close, restarts the
connector with no new data, and confirms nothing gets redelivered - the persisted
`processedTimestamp` correctly remembers that this window was already fully processed.

**`shouldPreserveOrderAcrossForcedKeyRangeSplit`**
Self-skips on the emulator - see above. Against real Spanner: inserts three rows at different
keys, then forces Spanner to physically split the key range around them (via the
`AddSplitPoints` admin API), then issues five rapid updates to each row while the split is
happening. Confirms every row's updates still arrive in the exact order they were written,
with no gaps or duplicates, even though the split forces the destination partitions through a
pause-and-resume handshake mid-stream.

**`shouldNotLoseOrReorderEventsWhenStoppedDuringForcedKeyRangeSplit`**
Self-skips on the emulator - see above. Against real Spanner: inserts a row, forces a
key-range split around it, and stops the connector immediately afterward (no settle time),
specifically trying to catch it mid-way through the pause-and-resume handshake a split
triggers. Restarts the connector and issues five more updates. Confirms every value from
before and after the restart still shows up, in the correct order (duplicates from
at-least-once redelivery are tolerated, but nothing may be missing or out of order) - proving
the in-progress handshake state survives a stop/start cycle correctly.

**`shouldNotLoseEventsWhenStoppedMidWindow`**
Inserts five rows, waits just long enough for the connector to start processing them (well
short of the sliding window's real-time close), then stops and restarts the connector.
Confirms all five rows eventually get delivered - nothing inserted before a mid-window stop
is silently skipped after restart.

**`shouldNotLoseDeleteWhenStoppedMidWindow`**
Same idea as the test above, but for a delete instead of an insert: inserts then deletes a
row, stops the connector mid-window, restarts it, and confirms the insert, the delete, and
the delete's tombstone all eventually arrive.

**`shouldDeliverAllModsFromLargeSingleTransaction`**
Inserts 20 rows as one single Spanner transaction (rather than 20 separate transactions).
Confirms all 20 resulting records are delivered, in commit order, and that every one of them
carries the same transaction ID - proving a large multi-row transaction isn't split up or
delivered out of order.

**`shouldAdvanceThroughQuietWindowWithoutStalling`**
Starts the connector, then waits through a full sliding window with no data changes in it at
all (only heartbeats). Inserts a row afterward and confirms it's delivered. Guards against
the connector getting stuck re-querying an empty window instead of advancing past it, which
would otherwise cause every later row to never be delivered.

**`shouldNotStartConnectorWithWindowMinutesTooLow`** / **`shouldNotStartConnectorWithWindowMinutesTooHigh`**
Two config-validation tests (no real table or change stream involved). Confirm the connector
refuses to start when `gcp.spanner.mutable.window.minutes` is set to 0 or to 31, since the
valid range is documented as 1-30 inclusive.

**`shouldCatchUpQuicklyThroughHistoricalWindows`**
Inserts three rows, then waits several minutes before ever starting the connector, with the
connector's start time pointed back at before the inserts happened - so several sliding
windows have already elapsed in real time before the connector reads its first row. Confirms
all three rows are still delivered, and that catching up through those already-elapsed
windows happens quickly (not by pacing one window per real-time minute the way a
live-tailing connector naturally would).

**`shouldPickUpSchemaChangeMidStream`**
Inserts a row, runs `ALTER TABLE ADD COLUMN` mid-stream (without touching the change stream's
own configuration or restarting the connector), inserts a second row using the new column,
then updates the first row's new column. Confirmed passing against a real Cloud Spanner
instance (`-Dspanner.test.real=true`) - twice.

**`shouldPickUpSchemaChangeMidStreamForNewInserts`**
Subset of the test above, for rows inserted after the schema change only: inserts a row, runs
`ALTER TABLE ADD COLUMN` mid-stream, then inserts a second row using the new column. Confirms
both inserts are delivered correctly - including the second one correctly showing the new
column's value - proving the schema change is picked up automatically without a restart or
reconfiguration. Deliberately doesn't touch the pre-existing-row-UPDATE path exercised by the
test above, keeping this scenario isolated to newly-inserted rows.

**`shouldResumeCorrectlyAfterWindowSizeIsChangedAcrossRestart`**
Starts the connector with one window size, inserts a row, confirms delivery, then stops the
connector and restarts it with a *different* `gcp.spanner.mutable.window.minutes` value using
the same connector name and offset file (a genuine resume of the same partition). Inserts a
second row and confirms it's still delivered. The window size lives only in the running
service instance, not in persisted offset state, so this proves a restart with a changed
window size still computes the next window correctly from wherever the partition left off.

## `PlacementMoveIT`

**Currently `@Disabled`**: `DROP PLACEMENT` alone can take minutes to hours on the shared
real-Spanner test instance, making iteration on this suite expensive, and requires the
`east-partition`/`west-partition` instance partitions to be pre-provisioned (see
[`doc/real-spanner-testing.md`](../doc/real-spanner-testing.md)).

Three real-Cloud-Spanner placement-move scenarios sharing a single `east`/`west` placement
pair, provisioned once for the whole class (one `@BeforeAll`/`@AfterAll`) since
`DROP PLACEMENT` alone can take minutes to hours on the shared test instance. Each test
still creates and drops its own tables/change stream inline.

**Runs against:** a real Cloud Spanner instance only - the class is
`@RealSpannerCompatible` and opts into real Spanner via
`-Dspanner.test.real=true` (see
[`doc/real-spanner-testing.md`](../doc/real-spanner-testing.md)). Requires
the pre-provisioned `east-partition`/`west-partition` instance partitions
described there too. `@BeforeAll` self-skips the whole class
(`Assumptions.assumeTrue`) unless `-Dspanner.test.real=true` is set, since
placement/geo-partitioning needs a real Cloud Spanner instance and the local
emulator rejects the placement DDL outright.

Spanner also restricts placement tables to a single `INSERT` or `DELETE` DML statement per
transaction during preview, so the two interleaved-child tests below issue the parent and
child inserts as separate transactions rather than combining them into one.

**`shouldOrderRecordsCorrectlyWhenRowMovesBetweenPlacements`**
Inserts a row in one geo-partitioned placement, moves it to another placement by updating
its placement key, then immediately issues a follow-up update to that same row. Confirms
the move event shows the correct before/after placement values, and that the follow-up
write's timestamp is strictly later than the move's - verifying the connector doesn't
deliver the follow-up out of order relative to the placement move. This is the first
geo-partitioned placement scenario confirmed working end to end through the actual
deployed connector.

**`shouldMoveInterleavedChildRowsWithParentPlacementChange`**
Inserts a parent row with an interleaved child row (the child has no placement key of
its own and always moves with its parent), then moves the parent between placements.
Confirms the parent's move event shows the new region, and that a follow-up update on the
child is correctly ordered after the parent's move timestamp - the only observable signal
that the child moved with its parent, since the connector doesn't surface a move as a Kafka
record or `SourceInfo` field for either the parent or the child. Confirmed passing against
real Spanner.

**`shouldOrderCascadingDeleteCorrectlyRelativeToInFlightPlacementMove`**
Inserts a parent row with an interleaved child (using `ON DELETE CASCADE`), moves the
parent to a different placement, then immediately deletes the parent so the delete cascades
to the child. Confirms both parent and child produce the expected
insert/move/delete/tombstone events, that the parent and child deletes share one
transaction ID, and that there's exactly one delete and one tombstone per key - verifying no
duplicate or dropped delete arises from the move and the cascade both needing to explain the
same row's disappearance at once. Confirmed passing against real Spanner.

## `TransactionRecordCountIT`

Parameterized across both partition modes; `@RealSpannerCompatible` with a connection/config
override (`-Dspanner.test.real=true`).

**Runs against:** the local Docker emulator by default. `MUTABLE_KEY_RANGE` self-skips there -
see Known Issues #2 below. Against real Spanner, both partition modes pass.

**`shouldReportRecordAndPartitionCountsForTransaction`**
Inserts one row in its own transaction, then in a separate transaction updates that row and
inserts a new row as two statements executed atomically together. Confirms the single-row
transaction reports a record count and partition count of 1, and that both records from the
two-statement transaction report a transaction-wide record count of 2 (not a count scoped to
just one row), while still correctly showing 1 partition. Confirmed passing against the
emulator (`IMMUTABLE_KEY_RANGE`) and real Spanner (both partition modes).

## Known Issues

### 1. Missing before-image backfill for partial `UPDATE` (real Spanner only)

Under `OLD_AND_NEW_VALUES` (the connector's only supported `value_capture_type`), real
Spanner's change-stream payload for an `UPDATE` includes only the columns that actually
changed - unmodified columns are simply absent from the JSON. `Mod.getOldValueNode`/
`getNewValueNode` do a plain key lookup with no fallback, and
`KafkaSpannerTableSchemaFactory`'s value-struct generators skip the field entirely when
that lookup returns `null`, so a missing column ends up `null` in the Kafka record instead
of its real value. The local emulator is more permissive (always includes the full row),
so this only manifests on real Spanner. The equivalent check on a DELETE's `before` struct
isn't affected, because Spanner always emits the full old row on a DELETE (there's no
"new" row to diff against) - it isn't that the connector has separate DELETE-side backfill
logic. Fixing this for UPDATE would need a last-known-row-value cache, keyed by primary
key, that the value-struct generators consult when a column is missing from the mod's
JSON; nothing like that exists in the connector today.

This surfaces differently depending on `value_capture_type`:

- `OLD_AND_NEW_VALUES` (default): both `old_values` and `new_values` are changed-columns-only
  for an UPDATE.
- `NEW_VALUES`: despite the name, `new_values` is *also* changed-columns-only for an UPDATE on
  real Spanner.
- `NEW_ROW`: `new_values` genuinely is the full new row unconditionally - the one
  `value_capture_type` unaffected by this bug.
- `NEW_ROW_AND_OLD_VALUES`: `old_values` is changed-columns-only (confirmed - the test fails
  before reaching its `after` assertions); `new_values` is unconfirmed here but likely
  full-row like `NEW_ROW`, going by the pattern above.

The following test scenarios are affected:

| Test | Partition mode(s) | Backend |
|---|---|---|
| `ChangeStreamCorrectContentIT.shouldCarryUnchangedColumnsThroughOnPartialUpdate` | `IMMUTABLE_KEY_RANGE`, `MUTABLE_KEY_RANGE` | Real Spanner |
| `ChangeStreamValueCaptureTypeIT.shouldCaptureFullNewRowWithNoNonKeyOldValues` (`NEW_VALUES`) | `IMMUTABLE_KEY_RANGE`, `MUTABLE_KEY_RANGE` | Real Spanner |
| `ChangeStreamValueCaptureTypeIT.shouldCaptureFullRowOnBothSides` (`NEW_ROW_AND_OLD_VALUES`) | `IMMUTABLE_KEY_RANGE`, `MUTABLE_KEY_RANGE` | Real Spanner |
| `ConcurrentKeysIT.shouldNotCrossContaminateStateBetweenInterleavedKeys` | `IMMUTABLE_KEY_RANGE`, `MUTABLE_KEY_RANGE` | Real Spanner |

### 2. Emulator's background partition split breaks `MUTABLE_KEY_RANGE` mid-test

The local emulator automatically re-splits partitions on a ~15-20 second timer. After a
background split, the connector doesn't pick up the new `MUTABLE_KEY_RANGE` child
partition for streaming quickly enough, so Spanner rejects the query with
`OUT_OF_RANGE: Specified start_timestamp is too far in the past`, and every retry reuses
the same now-stale timestamp, failing identically forever. `IMMUTABLE_KEY_RANGE` uses the
same generic split-handling code with no such delay and is unaffected. Real Spanner splits
by load, not a fixed schedule, so it doesn't hit this either. Leading suspect:
`PartitionManager`/`notifyMoveOut`, root cause not yet traced. This is a genuine bug in
the connector's handling of splits that happen naturally on their own schedule - distinct
from the emulator's separate, permanent inability to force a split via the
`AddSplitPoints` admin RPC.
The following test scenarios are affected:

| Test | Partition mode(s) | Backend |
|---|---|---|
| `CrossPartitionSplitOrderingIT.shouldDeliverFollowUpWriteExactlyOnceAndInOrderAcrossBackgroundPartitionSplits` | `MUTABLE_KEY_RANGE` | Emulator |
| `ConcurrentKeysIT.shouldNotCrossContaminateStateBetweenInterleavedKeys` | `MUTABLE_KEY_RANGE` | Emulator |
| `TransactionRecordCountIT.shouldReportRecordAndPartitionCountsForTransaction` | `MUTABLE_KEY_RANGE` | Emulator |

### 3. `MUTABLE_KEY_RANGE` restart/resume redelivers content instead of exactly once

Two distinct symptoms on the same test
(`ChangeStreamOrderingAndTransactionalIT.shouldResumeWithoutDuplicatingOrLosingContentAcrossRestart`),
one per backend - different enough that they look like two separate bugs rather than a
shared root cause.

**Emulator, `MUTABLE_KEY_RANGE` only**: after the restart, the connector doesn't deliver
the missed update once - it redelivers the same `before`/`after` content 5 times over
about 40 seconds, with the assigned task alternating between two task IDs each time (a
repeated rebalance pattern), before Spanner eventually rejects a query with
`OUT_OF_RANGE: Specified start_timestamp is too far in the future` - the mirror image of
the "too far in the past" error seen elsewhere (see issue 2), here from a retry's computed
start timestamp drifting ahead of Spanner's allowed window instead of behind it. Looks
like a genuine bug in the `MUTABLE_KEY_RANGE` restart/resume path (something keeps
re-triggering a rebalance and re-emitting the same buffered record instead of settling
into steady-state streaming) rather than a test issue. Root cause not yet investigated.

**Real Spanner, both partition modes**: a different, simpler symptom - exactly one
duplicate, not five. The pre-restart insert is redelivered unchanged alongside the
legitimate post-restart update, so the post-restart consume returns 2 records instead of
1 record. Since `IMMUTABLE_KEY_RANGE` hits this too, while the emulator's rebalance-storm symptom
above is `MUTABLE_KEY_RANGE`-only, these look like two separate issues rather than one
shared root cause. Root cause not yet investigated - could be a genuine at-least-once
duplicate this assertion is too strict to tolerate, or a gap in how the connector's
persisted offset accounts for a record delivered just before shutdown.

The following test scenarios are affected:

| Test | Partition mode(s) | Backend |
|---|---|---|
| `ChangeStreamOrderingAndTransactionalIT.shouldResumeWithoutDuplicatingOrLosingContentAcrossRestart` | `MUTABLE_KEY_RANGE` | Emulator |
| `ChangeStreamOrderingAndTransactionalIT.shouldResumeWithoutDuplicatingOrLosingContentAcrossRestart` | `IMMUTABLE_KEY_RANGE`, `MUTABLE_KEY_RANGE` | Real Spanner |

### 4. `ExcludeTtlDeletesFilterIT`'s pass may be inconclusive

The test's assertion (no delete/tombstone for the TTL-eligible row) can't distinguish
"the `exclude_ttl_deletes` filter actually suppressed a TTL delete" from "TTL garbage
collection just never fired within the test's wait window" - both produce an identical
observed record count. Passes everywhere, but isn't a confirmed proof of the filter.
The following test scenarios are affected:

| Test | Partition mode(s) | Backend |
|---|---|---|
| `ExcludeTtlDeletesFilterIT.shouldFilterOutTtlDeletesButStillDeliverUserIssuedDeletes` | `IMMUTABLE_KEY_RANGE`, `MUTABLE_KEY_RANGE` | Emulator & real Spanner |
