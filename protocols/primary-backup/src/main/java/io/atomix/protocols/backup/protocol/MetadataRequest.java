// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.backup.protocol;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Metadata request.
 */
public class MetadataRequest extends PrimaryBackupRequest {

  public static MetadataRequest request(String primitiveType) {
    return new MetadataRequest(primitiveType);
  }

  private final String primitiveType;

  private MetadataRequest(String primitiveType) {
    this.primitiveType = primitiveType;
  }

  public String primitiveType() {
    return primitiveType;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("primitiveType", primitiveType)
        .toString();
  }
}
