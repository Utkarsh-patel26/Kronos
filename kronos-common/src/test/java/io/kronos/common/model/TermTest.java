package io.kronos.common.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TermTest {

    @Test
    void zeroIsZero() {
        assertThat(Term.ZERO.value()).isZero();
    }

    @Test
    void incrementProducesNextTerm() {
        assertThat(Term.of(4).increment()).isEqualTo(Term.of(5));
    }

    @Test
    void comparisonIsByValue() {
        assertThat(Term.of(3).compareTo(Term.of(5))).isNegative();
        assertThat(Term.of(5).compareTo(Term.of(5))).isZero();
        assertThat(Term.of(7).compareTo(Term.of(5))).isPositive();

        assertThat(Term.of(7).isGreaterThan(Term.of(5))).isTrue();
        assertThat(Term.of(3).isLessThan(Term.of(5))).isTrue();
        assertThat(Term.of(5).isGreaterThan(Term.of(5))).isFalse();
        assertThat(Term.of(5).isLessThan(Term.of(5))).isFalse();
    }

    @Test
    void rejectsNegative() {
        assertThatThrownBy(() -> new Term(-1))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void toStringHasTPrefix() {
        assertThat(Term.of(42).toString()).isEqualTo("T42");
    }
}
