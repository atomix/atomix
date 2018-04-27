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

import io.atomix.utils.misc.ArraySizeHashPrinter;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Restore response.
 */
public class RestoreResponse extends PrimaryBackupResponse {

  public static RestoreResponse ok(long index, long timestamp, byte[] data) {
    return new RestoreResponse(Status.OK, index, timestamp, data);
  }

  public static RestoreResponse error() {
    return new RestoreResponse(Status.ERROR, 0, 0, null);
  }

  private final long index;
  private final long timestamp;
  private final byte[] data;

  private RestoreResponse(Status status, long index, long timestamp, byte[] data) {
    super(status);
    this.index = index;
    this.timestamp = timestamp;
    this.data = data;
  }

  public long index() {
    return index;
  }

  public long timestamp() {
    return timestamp;
  }

  public byte[] data() {
    return data;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("status", status())
        .add("index", index())
        .add("timestamp", timestamp())
        .add("data", data != null ? ArraySizeHashPrinter.of(data) : null)
        .toString();
  }
}
