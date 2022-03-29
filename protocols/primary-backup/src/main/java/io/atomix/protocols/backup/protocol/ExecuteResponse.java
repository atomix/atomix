// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
