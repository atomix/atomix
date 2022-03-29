// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage;

/**
 * Log exception.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StorageException extends RuntimeException {

  public StorageException() {
  }

  public StorageException(String message) {
    super(message);
  }

  public StorageException(String message, Throwable cause) {
    super(message, cause);
  }

  public StorageException(Throwable cause) {
    super(cause);
  }

  /**
   * Exception thrown when an entry being stored is too large.
   */
  public static class TooLarge extends StorageException {
    public TooLarge(String message) {
      super(message);
    }
  }

  /**
   * Exception thrown when storage runs out of disk space.
   */
  public static class OutOfDiskSpace extends StorageException {
    public OutOfDiskSpace(String message) {
      super(message);
    }
  }
}
