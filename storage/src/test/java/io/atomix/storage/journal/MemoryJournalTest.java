// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage.journal;

import io.atomix.storage.StorageLevel;

/**
 * Memory journal test.
 */
public class MemoryJournalTest extends AbstractJournalTest {
  public MemoryJournalTest(int maxSegmentSize, int cacheSize) {
    super(maxSegmentSize, cacheSize);
  }

  @Override
  protected StorageLevel storageLevel() {
    return StorageLevel.MEMORY;
  }
}
