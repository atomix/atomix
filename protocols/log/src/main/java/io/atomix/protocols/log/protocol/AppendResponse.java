// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.log.protocol;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Operation response.
 */
public class AppendResponse extends LogResponse {

  public static AppendResponse ok(long index) {
    return new AppendResponse(Status.OK, index);
  }

  public static AppendResponse error() {
    return new AppendResponse(Status.ERROR, 0);
  }

  private final long index;

  private AppendResponse(Status status, long index) {
    super(status);
    this.index = index;
  }

  public long index() {
    return index;
  }

  @Override
  public String toString() {
    if (status() == Status.OK) {
      return toStringHelper(this)
          .add("status", status())
          .add("index", index())
          .toString();
    } else {
      return toStringHelper(this)
          .add("status", status())
          .toString();
    }
  }
}
