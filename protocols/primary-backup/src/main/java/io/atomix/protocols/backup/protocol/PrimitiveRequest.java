// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.backup.protocol;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Primitive service request.
 */
public abstract class PrimitiveRequest extends PrimaryBackupRequest {
  private final PrimitiveDescriptor primitive;

  public PrimitiveRequest(PrimitiveDescriptor primitive) {
    this.primitive = primitive;
  }

  public PrimitiveDescriptor primitive() {
    return primitive;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("primitive", primitive)
        .toString();
  }
}
