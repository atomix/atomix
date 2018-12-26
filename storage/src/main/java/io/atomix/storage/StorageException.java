/*
 * Copyright 2015-present Open Networking Foundation
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
