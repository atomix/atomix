/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.protocols.log.partition;

import java.time.Duration;

import io.atomix.utils.memory.MemorySize;

/**
 * Log compaction configuration.
 */
public class LogCompactionConfig {
  private MemorySize size = MemorySize.from(1024 * 1024 * 1024);
  private Duration age;

  /**
   * Returns the maximum log size.
   *
   * @return the maximum log size
   */
  public MemorySize getSize() {
    return size;
  }

  /**
   * Sets the maximum log size.
   *
   * @param size the maximum log size
   * @return the log compaction configuration
   */
  public LogCompactionConfig setSize(MemorySize size) {
    this.size = size;
    return this;
  }

  /**
   * Returns the maximum log age.
   *
   * @return the maximum log age
   */
  public Duration getAge() {
    return age;
  }

  /**
   * Sets the maximum log age.
   *
   * @param age the maximum log age
   * @return the log compaction configuration
   */
  public LogCompactionConfig setAge(Duration age) {
    this.age = age;
    return this;
  }
}
