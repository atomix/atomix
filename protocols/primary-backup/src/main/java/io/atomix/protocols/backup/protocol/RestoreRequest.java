// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.backup.protocol;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Restore request.
 */
public class RestoreRequest extends PrimitiveRequest {

  public static RestoreRequest request(PrimitiveDescriptor primitive, long term) {
    return new RestoreRequest(primitive, term);
  }

  private final long term;

  public RestoreRequest(PrimitiveDescriptor primitive, long term) {
    super(primitive);
    this.term = term;
  }

  public long term() {
    return term;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("primitive", primitive())
        .add("term", term())
        .toString();
  }
}
