/*
 * Copyright 2014-present Open Networking Foundation
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
package io.atomix.cluster;

import io.atomix.utils.AbstractIdentifier;

import java.util.Objects;
import java.util.UUID;

/**
 * Node identifier.
 */
public class NodeId extends AbstractIdentifier<String> implements Comparable<NodeId> {

  /**
   * Creates a new cluster node identifier from the specified string.
   *
   * @return node id
   */
  public static NodeId anonymous() {
    return new NodeId(UUID.randomUUID().toString());
  }

  /**
   * Creates a new cluster node identifier from the specified string.
   *
   * @param id string identifier
   * @return node id
   */
  public static NodeId from(String id) {
    return new NodeId(id);
  }

  /**
   * Constructor for serialization.
   */
  private NodeId() {
    this("");
  }

  /**
   * Creates a new cluster node identifier from the specified string.
   *
   * @param id string identifier
   */
  public NodeId(String id) {
    super(id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id());
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof NodeId && ((NodeId) object).id().equals(id());
  }

  @Override
  public int compareTo(NodeId that) {
    return identifier.compareTo(that.identifier);
  }
}
