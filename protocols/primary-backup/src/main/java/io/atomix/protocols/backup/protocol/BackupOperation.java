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
package io.atomix.protocols.backup.protocol;

/**
 * Backup operation.
 */
public class BackupOperation {

  /**
   * Backup operation type.
   */
  public enum Type {

    /**
     * Execute operation.
     */
    EXECUTE,

    /**
     * Heartbeat operation.
     */
    HEARTBEAT,

    /**
     * Expire operation.
     */
    EXPIRE,

    /**
     * Close operation.
     */
    CLOSE,
  }

  private final Type type;
  private final long index;
  private final long timestamp;

  public BackupOperation(Type type, long index, long timestamp) {
    this.type = type;
    this.index = index;
    this.timestamp = timestamp;
  }

  public Type type() {
    return type;
  }

  public long index() {
    return index;
  }

  public long timestamp() {
    return timestamp;
  }
}
