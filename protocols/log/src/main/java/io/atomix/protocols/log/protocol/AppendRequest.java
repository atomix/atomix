// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.log.protocol;

import io.atomix.utils.misc.ArraySizeHashPrinter;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Operation request.
 */
public class AppendRequest extends LogRequest {

  public static AppendRequest request(byte[] value) {
    return new AppendRequest(value);
  }

  private final byte[] value;

  public AppendRequest(byte[] value) {
    this.value = value;
  }

  public byte[] value() {
    return value;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("value", ArraySizeHashPrinter.of(value))
        .toString();
  }
}
