/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.log.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.kuujo.copycat.log.LogException;

/**
 * Combines snapshot entries into a single snapshot.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SnapshotCombiner {
  private SnapshotStartEntry start;
  private List<SnapshotChunkEntry> chunks = new ArrayList<>();
  private SnapshotEndEntry end;

  /**
   * Sets the start entry of the snapshot entry set.
   *
   * @param entry The snapshot start entry.
   * @return The snapshot combiner.
   */
  public SnapshotCombiner withStart(SnapshotStartEntry entry) {
    this.start = entry;
    return this;
  }

  /**
   * Adds a chunk entry to the entry set.
   *
   * @param entry The snapshot chunk entry.
   * @return The snapshot combiner.
   */
  public SnapshotCombiner withChunk(SnapshotChunkEntry entry) {
    this.chunks.add(entry);
    return this;
  }

  /**
   * Adds a list of chunk entries to the entry set.
   *
   * @param entries A list of chunk entries.
   * @return The snapshot combiner.
   */
  public SnapshotCombiner withChunks(SnapshotChunkEntry... entries) {
    this.chunks.addAll(Arrays.asList(entries));
    return this;
  }

  /**
   * Adds a list of chunk entries to the entry set.
   *
   * @param entries A list of chunk entries.
   * @return The snapshot combiner.
   */
  public SnapshotCombiner withChunks(List<SnapshotChunkEntry> entries) {
    this.chunks.addAll(entries);
    return this;
  }

  /**
   * Sets the end entry of the snapshot entry set.
   *
   * @param entry The snapshot end entry.
   * @return The snapshot combiner.
   */
  public SnapshotCombiner withEnd(SnapshotEndEntry entry) {
    this.end = entry;
    return this;
  }

  /**
   * Combines the entries into a combined snapshot.
   *
   * @return A combined snapshot of all entries.
   */
  public CombinedSnapshot combine() {
    validate();
    long length = end.length();
    int i = 0;
    byte[] bytes = new byte[(int) length];
    for (SnapshotChunkEntry chunk : chunks) {
      System.arraycopy(chunk.data(), 0, bytes, i, chunk.data().length);
    }
    return new CombinedSnapshot(start.term(), start.cluster(), bytes);
  }

  /**
   * Validates that the entry set combines to form a valid snapshot.
   */
  private void validate() {
    if (start == null) {
      throw new LogException("No snapshot start applied");
    }
    if (chunks.isEmpty()) {
      throw new LogException("No snapshot data applied");
    }
    if (end == null) {
      throw new LogException("No snapshot end applied");
    }

    long term = start.term();
    long totalLength = 0;
    for (SnapshotChunkEntry chunk : chunks) {
      if (chunk.term() != term) {
        throw new LogException("Chunk term does not match start term");
      }
      totalLength += chunk.data().length;
    }

    if (end.term() != term) {
      throw new LogException("End term does not match start term");
    }

    if (totalLength != end.length()) {
      throw new LogException("Snapshot data length does not match expected length");
    }
  }

}
