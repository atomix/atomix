/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.log;

import java.util.List;
import java.util.Map;

/**
 * Snapshot manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface SnapshotManager {

  /**
   * Returns a list of all available snapshots.
   *
   * @return A list of all available snapshots.
   */
  List<SnapshotInfo> listSnapshots();

  /**
   * Loads a snapshot by ID.
   *
   * @param id The unique snapshot ID.
   * @return The loaded snapshot.
   */
  Map<String, Object> readSnapshot(String id);

  /**
   * Stores a new snapshot.
   *
   * @param snapshot The snapshot to store.
   * @return The stored snapshot info.
   */
  SnapshotInfo writeSnapshot(Map<String, Object> snapshot);

}
