// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage.journal.index;

import java.util.Map;
import java.util.TreeMap;

/**
 * Sparse index.
 */
public class SparseJournalIndex implements JournalIndex {
  private static final int MIN_DENSITY = 1000;
  private final int density;
  private final TreeMap<Long, Integer> positions = new TreeMap<>();

  public SparseJournalIndex(double density) {
    this.density = (int) Math.ceil(MIN_DENSITY / (density * MIN_DENSITY));
  }

  @Override
  public void index(long index, int position) {
    if (index % density == 0) {
      positions.put(index, position);
    }
  }

  @Override
  public Position lookup(long index) {
    Map.Entry<Long, Integer> entry = positions.floorEntry(index);
    return entry != null ? new Position(entry.getKey(), entry.getValue()) : null;
  }

  @Override
  public void truncate(long index) {
    positions.tailMap(index, false).clear();
  }
}
