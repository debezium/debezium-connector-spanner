/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model.event;

/**
 * Utilities for interpreting Spanner's {@code record_sequence} field, which comes in two
 * observed formats depending on the change stream's partition mode: a plain hex number with no
 * discriminator (e.g. {@code "00000001"}) on change streams that don't use
 * {@code MUTABLE_KEY_RANGE} partitioning, and a hex {@code "<hi>-<lo>"} composite (e.g.
 * {@code "963d1af435fb3e79-00000000"}) on {@code MUTABLE_KEY_RANGE} change streams, where
 * {@code hi} is a per-transaction discriminator and {@code lo} orders records within that
 * transaction. A plain (unseparated) value is treated as {@code hi=0}, i.e. as if it had no
 * discriminator.
 */
public final class RecordSequenceUtils {

    private RecordSequenceUtils() {
    }

    /**
     * Splits a {@code record_sequence} value into its {@code hi}/{@code lo} unsigned 64-bit
     * components. Handles both the unseparated plain-hex format (no {@code hi}, treated as
     * {@code 0}) and the separated {@code "<hi>-<lo>"} composite format.
     */
    private static long[] splitHiLo(String sequence) {
        String[] parts = sequence.split("-", 2);
        long hi = Long.parseUnsignedLong(parts[0], 16);
        if (parts.length == 1) {
            return new long[]{ 0L, hi };
        }

        long lo = Long.parseUnsignedLong(parts[1], 16);
        return new long[]{ hi, lo };
    }

    /**
     * Parses a {@code record_sequence} value into a single comparable {@link Long}, for contexts
     * (e.g. the informational {@code SourceInfo} "sequence" field) that need one numeric value
     * rather than a proper comparison. This packs {@code hi}'s lower 32 bits together with
     * {@code lo}'s lower 32 bits, which is lossy for a {@code hi} wider than 32 bits - use
     * {@link #compare} instead wherever correctness of ordering actually matters.
     *
     * <p>Falls back to {@link Long#parseLong} when {@code sequence} has no {@code "-"} (i.e. isn't
     * the hyphenated hex {@code "<hi>-<lo>"} composite), e.g. a plain decimal sequence from a
     * stream that isn't {@code MUTABLE_KEY_RANGE}.
     */
    public static Long parseSequenceNumber(String sequence) {
        if (sequence == null) {
            return null;
        }
        if (!sequence.contains("-")) {
            return Long.parseLong(sequence);
        }
        long[] hiLo = splitHiLo(sequence);
        return hiLo[1];
    }

    /**
     * Parses a {@code record_sequence} value into its {@code hi} component (the per-transaction
     * discriminator) as a raw hex string, for the public {@code SourceInfo}
     * "record_sequence_prefix" field. Returns {@code null} for the unseparated plain-hex format,
     * which has no discriminator to extract.
     */
    public static String parseSequencePrefix(String sequence) {
        if (sequence == null) {
            return null;
        }
        int dashIndex = sequence.indexOf('-');
        if (dashIndex < 0) {
            return null;
        }
        return sequence.substring(0, dashIndex);
    }

    /**
     * Compares two non-null {@code record_sequence} values by comparing {@code hi} and
     * {@code lo} as an unsigned 64-bit tuple - {@code hi} first, then {@code lo} - rather than
     * packing them into a single value, which would require masking {@code hi} down to 32 bits
     * and lose precision for a full 64-bit discriminator.
     */
    public static int compare(String a, String b) {
        long[] hiLoA = splitHiLo(a);
        long[] hiLoB = splitHiLo(b);
        int cmp = Long.compareUnsigned(hiLoA[0], hiLoB[0]);
        return cmp != 0 ? cmp : Long.compareUnsigned(hiLoA[1], hiLoB[1]);
    }
}
