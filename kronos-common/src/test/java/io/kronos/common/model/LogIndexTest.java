package io.kronos.common.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LogIndexTest {

    @Test
    void incrementAndDecrement() {
        assertThat(LogIndex.of(4).increment()).isEqualTo(LogIndex.of(5));
        assertThat(LogIndex.of(4).decrement()).isEqualTo(LogIndex.of(3));
    }

    @Test
    void plus() {
        assertThat(LogIndex.of(10).plus(5)).isEqualTo(LogIndex.of(15));
    }

    @Test
    void decrementZeroThrows() {
        assertThatThrownBy(LogIndex.ZERO::decrement)
            .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void ordering() {
        assertThat(LogIndex.of(3).isLessThan(LogIndex.of(5))).isTrue();
        assertThat(LogIndex.of(7).isGreaterThan(LogIndex.of(5))).isTrue();
        assertThat(LogIndex.of(5).compareTo(LogIndex.of(5))).isZero();
    }

    @Test
    void rejectsNegative() {
        assertThatThrownBy(() -> new LogIndex(-1))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void toStringHasHashPrefix() {
        assertThat(LogIndex.of(9).toString()).isEqualTo("#9");
    }
}
