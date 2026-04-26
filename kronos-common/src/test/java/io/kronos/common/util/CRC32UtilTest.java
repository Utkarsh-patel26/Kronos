package io.kronos.common.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CRC32UtilTest {

    @Test
    void computeIsDeterministic() {
        byte[] data = "kronos".getBytes();
        long a = CRC32Util.compute(data);
        long b = CRC32Util.compute(data);
        assertThat(a).isEqualTo(b);
    }

    @Test
    void computeOverSubRangeMatchesFullWhenRangesEqual() {
        byte[] data = {1, 2, 3, 4, 5};
        assertThat(CRC32Util.compute(data))
            .isEqualTo(CRC32Util.compute(data, 0, data.length));
    }

    @Test
    void verifyMatchesCompute() {
        byte[] data = "hello".getBytes();
        long crc = CRC32Util.compute(data);
        assertThat(CRC32Util.verify(data, crc)).isTrue();
        assertThat(CRC32Util.verify(data, crc ^ 1)).isFalse();
    }
}
