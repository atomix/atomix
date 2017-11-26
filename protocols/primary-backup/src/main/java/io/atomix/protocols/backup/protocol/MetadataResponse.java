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
