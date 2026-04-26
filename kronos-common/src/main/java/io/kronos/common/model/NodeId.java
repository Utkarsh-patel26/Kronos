package io.kronos.common.model;

import java.util.Objects;

/**
 * A node's stable identity inside a Kronos cluster.
 *
 * <p>Wraps a String so the compiler can tell a NodeId apart from arbitrary
 * strings passed around the codebase. Two NodeIds are equal iff their values
 * are equal.
 */
public record NodeId(String value) {

    public NodeId {
        Objects.requireNonNull(value, "NodeId value must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException("NodeId value must not be blank");
        }
    }

    public static NodeId of(String value) {
        return new NodeId(value);
    }

    @Override
    public String toString() {
        return value;
    }
}
