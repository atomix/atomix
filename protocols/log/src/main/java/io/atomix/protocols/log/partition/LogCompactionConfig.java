// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
