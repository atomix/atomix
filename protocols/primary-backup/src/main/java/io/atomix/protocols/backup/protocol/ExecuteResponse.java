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
 * Operation response.
 */
public class ExecuteResponse extends PrimaryBackupResponse {

  public static ExecuteResponse ok(byte[] result) {
    return new ExecuteResponse(Status.OK, result);
  }

  public static ExecuteResponse error() {
    return new ExecuteResponse(Status.ERROR, null);
  }

  private final byte[] result;

  private ExecuteResponse(Status status, byte[] result) {
    super(status);
    this.result = result;
  }

  public byte[] result() {
    return result;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("status", status())
        .add("result", result != null ? ArraySizeHashPrinter.of(result) : null)
        .toString();
  }
}
