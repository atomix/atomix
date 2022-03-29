// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage.journal;

import io.atomix.storage.StorageLevel;

/**
 * Disk journal test.
 */
public class DiskJournalTest extends PersistentJournalTest {
  public DiskJournalTest(int maxSegmentSize, int cacheSize) {
    super(maxSegmentSize, cacheSize);
  }

  @Override
  protected StorageLevel storageLevel() {
    return StorageLevel.DISK;
  }
}
