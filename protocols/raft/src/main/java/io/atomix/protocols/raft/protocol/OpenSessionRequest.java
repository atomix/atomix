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
package io.atomix.protocols.raft.protocol;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.protocols.raft.ReadConsistency;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Open session request.
 */
public class OpenSessionRequest extends AbstractRaftRequest {

  /**
   * Returns a new open session request builder.
   *
   * @return A new open session request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final String node;
  private final String name;
  private final String typeName;
  private final ReadConsistency readConsistency;
  private final long minTimeout;
  private final long maxTimeout;

  public OpenSessionRequest(String node, String name, String typeName, ReadConsistency readConsistency, long minTimeout, long maxTimeout) {
    this.node = node;
    this.name = name;
    this.typeName = typeName;
    this.readConsistency = readConsistency;
    this.minTimeout = minTimeout;
    this.maxTimeout = maxTimeout;
  }

  /**
   * Returns the client node identifier.
   *
   * @return The client node identifier.
   */
  public String node() {
    return node;
  }

  /**
   * Returns the state machine name.
   *
   * @return The state machine name.
   */
  public String serviceName() {
    return name;
  }

  /**
   * Returns the state machine type;
   *
   * @return The state machine type.
   */
  public String serviceType() {
    return typeName;
  }

  /**
   * Returns the session read consistency level.
   *
   * @return The session's read consistency.
   */
  public ReadConsistency readConsistency() {
    return readConsistency;
  }

  /**
   * Returns the minimum session timeout.
   *
   * @return The minimum session timeout.
   */
  public long minTimeout() {
    return minTimeout;
  }

  /**
   * Returns the maximum session timeout.
   *
   * @return The maximum session timeout.
   */
  public long maxTimeout() {
    return maxTimeout;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), name, typeName, minTimeout, maxTimeout);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof OpenSessionRequest) {
      OpenSessionRequest request = (OpenSessionRequest) object;
      return request.node.equals(node)
          && request.name.equals(name)
          && request.typeName.equals(typeName)
          && request.readConsistency == readConsistency
          && request.minTimeout == minTimeout
          && request.maxTimeout == maxTimeout;
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("node", node)
        .add("serviceName", name)
        .add("serviceType", typeName)
        .add("readConsistency", readConsistency)
        .add("minTimeout", minTimeout)
        .add("maxTimeout", maxTimeout)
        .toString();
  }

  /**
   * Open session request builder.
   */
  public static class Builder extends AbstractRaftRequest.Builder<Builder, OpenSessionRequest> {
    private String nodeId;
    private String serviceName;
    private String serviceType;
    private ReadConsistency readConsistency = ReadConsistency.LINEARIZABLE;
    private long minTimeout;
    private long maxTimeout;

    /**
     * Sets the client node identifier.
     *
     * @param node The client node identifier.
     * @return The open session request builder.
     * @throws NullPointerException if {@code node} is {@code null}
     */
    public Builder withNodeId(MemberId node) {
      this.nodeId = checkNotNull(node, "node cannot be null").id();
      return this;
    }

    /**
     * Sets the service name.
     *
     * @param serviceName The service name.
     * @return The open session request builder.
     * @throws NullPointerException if {@code serviceName} is {@code null}
     */
    public Builder withServiceName(String serviceName) {
      this.serviceName = checkNotNull(serviceName, "serviceName cannot be null");
      return this;
    }

    /**
     * Sets the service type name.
     *
     * @param primitiveType The service type name.
     * @return The open session request builder.
     * @throws NullPointerException if {@code serviceType} is {@code null}
     */
    public Builder withServiceType(PrimitiveType primitiveType) {
      this.serviceType = checkNotNull(primitiveType, "serviceType cannot be null").id();
      return this;
    }

    /**
     * Sets the session read consistency.
     *
     * @param readConsistency the session read consistency
     * @return the session request builder
     * @throws NullPointerException if the {@code readConsistency} is null
     */
    public Builder withReadConsistency(ReadConsistency readConsistency) {
      this.readConsistency = checkNotNull(readConsistency, "readConsistency cannot be null");
      return this;
    }

    /**
     * Sets the minimum session timeout.
     *
     * @param timeout The minimum session timeout.
     * @return The open session request builder.
     * @throws IllegalArgumentException if {@code timeout} is not positive
     */
    public Builder withMinTimeout(long timeout) {
      checkArgument(timeout >= 0, "timeout must be positive");
      this.minTimeout = timeout;
      return this;
    }

    /**
     * Sets the maximum session timeout.
     *
     * @param timeout The maximum session timeout.
     * @return The open session request builder.
     * @throws IllegalArgumentException if {@code timeout} is not positive
     */
    public Builder withMaxTimeout(long timeout) {
      checkArgument(timeout >= 0, "timeout must be positive");
      this.maxTimeout = timeout;
      return this;
    }

    @Override
    protected void validate() {
      super.validate();
      checkNotNull(nodeId, "client cannot be null");
      checkNotNull(serviceName, "name cannot be null");
      checkNotNull(serviceType, "typeName cannot be null");
      checkArgument(minTimeout >= 0, "minTimeout must be positive");
      checkArgument(maxTimeout >= 0, "maxTimeout must be positive");
    }

    /**
     * @throws IllegalStateException is session is not positive
     */
    @Override
    public OpenSessionRequest build() {
      validate();
      return new OpenSessionRequest(nodeId, serviceName, serviceType, readConsistency, minTimeout, maxTimeout);
    }
  }
}
