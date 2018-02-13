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
package io.atomix.storage.journal;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Journal segment cache.
 */
class JournalSegmentCache {
  private final int size;
  private final Map<Long, Indexed> entries;
  private long firstIndex;

  JournalSegmentCache(long index, int size) {
    this.size = size;
    this.entries = new HashMap<>(size);
    this.firstIndex = index;
  }

  /**
   * Adds an entry to the cache.
   *
   * @param indexed the entry to add to the cache
   */
  public void put(Indexed indexed) {
    if (indexed.index() == firstIndex + entries.size()) {
      entries.put(indexed.index(), indexed);
    }
    if (entries.size() > size) {
      entries.remove(firstIndex);
      firstIndex++;
    }
  }

  /**
   * Gets an entry from the cache.
   *
   * @param index the index of the entry to lookup
   * @return the indexed entry
   */
  public Indexed get(long index) {
    return entries.get(index);
  }

  /**
   * Truncates the cache to the given index.
   *
   * @param index the index to which to truncate the cache
   */
  public void truncate(long index) {
    if (index < firstIndex) {
      firstIndex = index + 1;
      entries.clear();
    } else {
      int size = entries.size();
      for (long i = index + 1; i < firstIndex + size; i++) {
        entries.remove(i);
      }
    }
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("size", size)
        .toString();
  }
}
