package io.kronos.storage.snapshot;

import io.kronos.common.model.LogIndex;
import io.kronos.common.model.Term;

import java.nio.file.Path;

public record SnapshotMetadata(LogIndex lastIncludedIndex, Term lastIncludedTerm, Path path) {}
