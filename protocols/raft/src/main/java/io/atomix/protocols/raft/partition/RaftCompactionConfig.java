// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.partition;

/**
 * Raft compaction configuration.
 */
public class RaftCompactionConfig {
  private static final boolean DEFAULT_DYNAMIC_COMPACTION = true;
  private static final double DEFAULT_FREE_DISK_BUFFER = .2;
  private static final double DEFAULT_FREE_MEMORY_BUFFER = .2;

  private boolean dynamic = DEFAULT_DYNAMIC_COMPACTION;
  private double freeDiskBuffer = DEFAULT_FREE_DISK_BUFFER;
  private double freeMemoryBuffer = DEFAULT_FREE_MEMORY_BUFFER;

  /**
   * Returns whether dynamic compaction is enabled.
   *
   * @return whether dynamic compaction is enabled
   */
  public boolean isDynamic() {
    return dynamic;
  }

  /**
   * Sets whether dynamic compaction is enabled.
   *
   * @param dynamic whether dynamic compaction is enabled
   * @return the compaction configuration
   */
  public RaftCompactionConfig setDynamic(boolean dynamic) {
    this.dynamic = dynamic;
    return this;
  }

  /**
   * Returns the free disk buffer.
   *
   * @return the free disk buffer
   */
  public double getFreeDiskBuffer() {
    return freeDiskBuffer;
  }

  /**
   * Sets the free disk buffer.
   *
   * @param freeDiskBuffer the free disk buffer
   * @return the compaction configuration
   */
  public RaftCompactionConfig setFreeDiskBuffer(double freeDiskBuffer) {
    this.freeDiskBuffer = freeDiskBuffer;
    return this;
  }

  /**
   * Returns the free memory buffer.
   *
   * @return the free memory buffer
   */
  public double getFreeMemoryBuffer() {
    return freeMemoryBuffer;
  }

  /**
   * Sets the free memory buffer.
   *
   * @param freeMemoryBuffer the free memory buffer
   * @return the compaction configuration
   */
  public RaftCompactionConfig setFreeMemoryBuffer(double freeMemoryBuffer) {
    this.freeMemoryBuffer = freeMemoryBuffer;
    return this;
  }
}
