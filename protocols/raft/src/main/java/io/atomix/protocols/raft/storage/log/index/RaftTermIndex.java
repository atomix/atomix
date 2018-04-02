/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.protocols.raft.storage.log.index;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Raft log term index.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class RaftTermIndex {
  private final TreeMap<Long, Long> terms = new TreeMap<>();

  /**
   * Indexes the given index with the given term.
   *
   * @param term  The term to index.
   * @param index the starting index of the given term
   */
  public void index(long term, long index) {
    terms.put(term, index);
  }

  /**
   * Looks up the index for the given term.
   *
   * @param term The term for which to look up the index.
   * @return The first index for the given term.
   */
  public long lookup(long term) {
    return terms.getOrDefault(term, 0L);
  }

  /**
   * Truncates the index to the given index.
   *
   * @param index The entry index to which to truncate the index.
   */
  public void truncate(long index) {
    terms.entrySet().removeIf(entry -> entry.getValue() > index);
  }

  /**
   * Compacts the head of the index to the given index.
   *
   * @param index the entry index to which to compact the index
   */
  public void compact(long index) {
    Iterator<Map.Entry<Long, Long>> iterator = terms.entrySet().iterator();
    Map.Entry<Long, Long> lastEntry = null;
    while (iterator.hasNext()) {
      Map.Entry<Long, Long> entry = iterator.next();
      if (entry.getValue() < index) {
        iterator.remove();
        lastEntry = entry;
      } else {
        break;
      }
    }

    if (lastEntry != null) {
      Map.Entry<Long, Long> firstEntry = terms.firstEntry();
      if (firstEntry == null || firstEntry.getValue() > index) {
        terms.put(lastEntry.getKey(), index);
      }
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
