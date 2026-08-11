/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model.event;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Verifies record_sequence handling against both real-world formats observed from Spanner: the
 * unseparated plain hex value with no discriminator (e.g. {@code "00000001"}), seen on change
 * streams that don't use {@code MUTABLE_KEY_RANGE} partitioning, and the separated hex
 * {@code "<hi>-<lo>"} composite (e.g. {@code "963d1af435fb3e79-00000000"}), seen on
 * {@code MUTABLE_KEY_RANGE} change streams.
 */
class RecordSequenceUtilsTest {

    @Test
    void comparesHexHiLoCompositeSequencesConsistently() {
        String earlier = "0000000000000000-00000001";
        String later = "0000000000000000-00000002";
        assertTrue(RecordSequenceUtils.compare(later, earlier) > 0);
        assertEquals(0, RecordSequenceUtils.compare(earlier, earlier));
    }

    /**
     * Regression test: packing {@code hi} and {@code lo} into a single 64-bit value via
     * {@code (hi << 32) | lo} requires masking {@code hi} down to 32 bits, which silently
     * discards the upper half of a full 64-bit discriminator. Two records whose discriminators
     * differ only in the upper 32 bits (but share the same lower 32 bits and the same {@code lo})
     * would incorrectly compare as equal under that scheme. {@code compare} must distinguish
     * them by treating {@code hi} as a full unsigned 64-bit value.
     */
    @Test
    void distinguishesHexCompositesDifferingOnlyInUpperBitsOfDiscriminator() {
        String discriminatorA = "963d1af435fb3e79-00000000";
        String discriminatorB = "0000000035fb3e79-00000000";

        assertTrue(RecordSequenceUtils.compare(discriminatorA, discriminatorB) != 0,
                "discriminators differing only in the upper 32 bits must not compare as equal");
    }

    /**
     * Regression test: a full 64-bit {@code hi} discriminator must take priority over {@code lo}
     * in the comparison, exactly as a lexicographic tuple (hi, lo) comparison would - not get
     * corrupted by combining both into one packed value.
     */
    @Test
    void ordersHexCompositesByDiscriminatorFirstThenByLowerPart() {
        String smallHiLargeLo = "0000000000000000-ffffffff";
        String largeHiSmallLo = "0000000000000001-00000000";

        assertTrue(RecordSequenceUtils.compare(largeHiSmallLo, smallHiLargeLo) > 0,
                "a larger discriminator must always sort after a smaller one, regardless of the lower part");
    }

    @Test
    void parsesOpaqueHexCompositeDeterministicallyWithoutThrowing() {
        String sequence = "963d1af435fb3e79-00000000";
        Long parsed = RecordSequenceUtils.parseSequenceNumber(sequence);
        assertEquals(parsed, RecordSequenceUtils.parseSequenceNumber(sequence));
    }

    @Test
    void returnsNullForNullInput() {
        assertEquals(null, RecordSequenceUtils.parseSequenceNumber(null));
    }

    /**
     * Regression test for the public {@code source.sequence} field: Debezium's documentation
     * describes it as this record's number within its transaction, i.e. {@code lo} alone.
     * {@code hi} is a transaction id and two records from different transactions (different {@code hi})
     * sharing the same in-transaction position ({@code lo}) must parse to the same "sequence" value.
     */
    @Test
    void parseSequenceNumberIgnoresDiscriminatorAndReturnsOnlyLo() {
        String transactionA = "963d1af435fb3e79-00000005";
        String transactionB = "0000000000000001-00000005";

        assertEquals(5L, RecordSequenceUtils.parseSequenceNumber(transactionA));
        assertEquals(RecordSequenceUtils.parseSequenceNumber(transactionA), RecordSequenceUtils.parseSequenceNumber(transactionB));
    }

    /**
     * Unseparated record_sequence values have no {@code "<hi>-<lo>"} separator at all - the whole
     * string is the sequence number, with no discriminator to strip out.
     */
    @Test
    void parseSequenceNumberHandlesUnseparatedPlainHexFormat() {
        assertEquals(1L, RecordSequenceUtils.parseSequenceNumber("00000001"));
    }

    /**
     * An unseparated value and an equivalent separated value with an all-zero {@code hi} must
     * parse to the same sequence number - an unseparated value is just a separated value with no
     * discriminator.
     */
    @Test
    void unseparatedAndEquivalentSeparatedSequenceParseToTheSameSequenceNumber() {
        assertEquals(RecordSequenceUtils.parseSequenceNumber("00000001"),
                RecordSequenceUtils.parseSequenceNumber("0000000000000000-00000001"));
    }

    /**
     * Unseparated values must also compare correctly against {@link RecordSequenceUtils#compare},
     * not just against themselves. An unseparated value and a separated value sharing the same
     * {@code lo} but differing in {@code hi} must not compare as equal.
     */
    @Test
    void unseparatedSequenceWithDifferentDiscriminatorThanSeparatedDoesNotCompareEqual() {
        String unseparated = "00000001";
        String separatedWithNonZeroHi = "0000000000000001-00000001";

        assertTrue(RecordSequenceUtils.compare(unseparated, separatedWithNonZeroHi) != 0,
                "an unseparated value (hi=0) must not compare equal to a separated value with a non-zero hi, even with the same lo");
    }

    /**
     * Regression test: {@code record_sequence} values from streams that aren't
     * {@code MUTABLE_KEY_RANGE} (e.g. from the Spanner emulator) can be plain decimal, with no
     * {@code "-"} hi/lo separator. {@code parseSequenceNumber} must fall back to
     * {@link Long#parseLong} for these instead of throwing {@code ArrayIndexOutOfBoundsException}
     * from the hyphenated hi/lo split.
     */
    @Test
    void fallsBackToPlainDecimalParsingWhenSequenceHasNoHyphen() {
        String sequence = "42";
        assertEquals(42L, RecordSequenceUtils.parseSequenceNumber(sequence));
    }
}
