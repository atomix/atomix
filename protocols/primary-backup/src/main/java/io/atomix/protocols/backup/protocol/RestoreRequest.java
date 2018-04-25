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

import io.atomix.cluster.NodeId;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Restore request.
 */
public class RestoreRequest extends PrimitiveRequest {

  public static RestoreRequest request(PrimitiveDescriptor primitive, NodeId nodeId, long term) {
    return new RestoreRequest(primitive, nodeId, term);
  }

  private final long term;
  private final NodeId nodeId;

  public RestoreRequest(PrimitiveDescriptor primitive, NodeId nodeId, long term) {
    super(primitive);
    this.term = term;
    this.nodeId = nodeId;
  }

  public long term() {
    return term;
  }

  public NodeId nodeId() {
    return nodeId;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("primitive", primitive())
        .add("nodeId", nodeId())
        .add("term", term())
        .toString();
  }
}
