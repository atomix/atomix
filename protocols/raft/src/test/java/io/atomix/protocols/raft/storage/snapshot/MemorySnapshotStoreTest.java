// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.storage.snapshot;

import io.atomix.protocols.raft.storage.RaftStorage;
import io.atomix.storage.StorageLevel;

/**
 * Memory snapshot store test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MemorySnapshotStoreTest extends AbstractSnapshotStoreTest {

  /**
   * Returns a new snapshot store.
   */
  protected SnapshotStore createSnapshotStore() {
    RaftStorage storage = RaftStorage.builder()
        .withPrefix("test")
        .withStorageLevel(StorageLevel.MEMORY)
        .build();
    return new SnapshotStore(storage);
  }

}
