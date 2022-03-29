// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.backup.protocol;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Operation request.
 */
public class CloseRequest extends PrimitiveRequest {

  public static CloseRequest request(PrimitiveDescriptor primitive, long session) {
    return new CloseRequest(primitive, session);
  }

  private final long session;

  public CloseRequest(PrimitiveDescriptor primitive, long session) {
    super(primitive);
    this.session = session;
  }

  public long session() {
    return session;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("primitive", primitive())
        .add("session", session())
        .toString();
  }
}
