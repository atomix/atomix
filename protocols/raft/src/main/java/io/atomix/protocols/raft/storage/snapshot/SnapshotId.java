/*
 * Copyright 2017-present Open Networking Laboratory
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
package io.atomix.protocols.raft.storage.snapshot;

import io.atomix.utils.Identifier;

/**
 * Snapshot identifier.
 */
public class SnapshotId extends Identifier<Long> {

  /**
   * Creates a snapshot ID from the given number.
   *
   * @param id the number from which to create the identifier
   * @return the snapshot identifier
   */
  public static SnapshotId of(long id) {
    return new SnapshotId(id);
  }

  /**
   * Creates a snapshot ID from the given string.
   *
   * @param id the string from which to create the identifier
   * @return the snapshot identifier
   */
  public static SnapshotId of(String id) {
    return of(Long.parseLong(id));
  }

  public SnapshotId(Long value) {
    super(value);
  }
}
