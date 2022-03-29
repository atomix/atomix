// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage.statistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * Atomix storage statistics.
 */
public class StorageStatistics {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageStatistics.class);

  private final File file;
  private final MBeanServer mBeanServer;

  public StorageStatistics(File file) {
    this.file = file;
    this.mBeanServer = ManagementFactory.getPlatformMBeanServer();
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

  /**
   * Returns the amount of free memory remaining.
   *
   * @return the amount of free memory remaining if successful, -1 return indicates failure.
   */
  public long getFreeMemory() {
    try {
      return (long) mBeanServer.getAttribute(new ObjectName("java.lang", "type", "OperatingSystem"), "FreePhysicalMemorySize");
    } catch (Exception e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("An exception occurred during memory check", e);
      }
    }
    return -1;
  }

  /**
   * Returns the total amount of memory.
   *
   * @return the total amount of memory if successful, -1 return indicates failure.
   */
  public long getTotalMemory() {
    try {
      return (long) mBeanServer.getAttribute(new ObjectName("java.lang", "type", "OperatingSystem"), "TotalPhysicalMemorySize");
    } catch (Exception e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("An exception occurred during memory check", e);
      }
    }
    return -1;
  }
}
