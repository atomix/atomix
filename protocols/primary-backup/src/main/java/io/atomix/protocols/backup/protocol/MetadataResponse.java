// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.backup.protocol;

import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Metadata response.
 */
public class MetadataResponse extends PrimaryBackupResponse {

  public static MetadataResponse ok(Set<String> primitiveNames) {
    return new MetadataResponse(Status.OK, primitiveNames);
  }

  public static MetadataResponse error() {
    return new MetadataResponse(Status.ERROR, null);
  }

  private final Set<String> primitiveNames;

  private MetadataResponse(Status status, Set<String> primitiveNames) {
    super(status);
    this.primitiveNames = primitiveNames;
  }

  public Set<String> primitiveNames() {
    return primitiveNames;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("status", status())
        .add("primitiveNames", primitiveNames)
        .toString();
  }
}
