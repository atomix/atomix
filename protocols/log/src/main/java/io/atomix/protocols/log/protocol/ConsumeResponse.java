// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.log.protocol;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Read response.
 */
public class ConsumeResponse extends LogResponse {

  public static ConsumeResponse ok() {
    return new ConsumeResponse(Status.OK);
  }

  public static ConsumeResponse error() {
    return new ConsumeResponse(Status.ERROR);
  }

  private ConsumeResponse(Status status) {
    super(status);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("status", status())
        .toString();
  }
}
