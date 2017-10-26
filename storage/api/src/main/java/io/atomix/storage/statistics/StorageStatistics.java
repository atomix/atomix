/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.storage.statistics;

import java.io.File;

/**
 * Atomix storage statistics.
 */
public class StorageStatistics {
  private final File file;

  public StorageStatistics(File file) {
    this.file = file;
  }

  /**
   * Returns the amount of usable space remaining.
   *
   * @return the amount of usable space remaining
   */
  public long getUsableSpace() {
    return file.getUsableSpace();
  }

  /**
   * Returns the amount of free space remaining.
   *
   * @return the amount of free space remaining
   */
  public long getFreeSpace() {
    return file.getFreeSpace();
  }

  /**
   * Returns the total amount of space.
   *
   * @return the total amount of space
   */
  public long getTotalSpace() {
    return file.getTotalSpace();
  }
}
