// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage.journal;

import io.atomix.storage.StorageLevel;

/**
 * Memory mapped journal test.
 */
public class MappedJournalTest extends PersistentJournalTest {
  public MappedJournalTest(int maxSegmentSize, int cacheSize) {
    super(maxSegmentSize, cacheSize);
  }

  @Override
  protected StorageLevel storageLevel() {
    return StorageLevel.MAPPED;
  }
}
