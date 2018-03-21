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

import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.protocols.raft.service.PropagationStrategy;
import io.atomix.protocols.raft.service.ServiceType;

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
  public static Builder newBuilder() {
    return new Builder();
  }

  private final String member;
  private final String name;
  private final String typeName;
  private final ReadConsistency readConsistency;
  private final long minTimeout;
  private final long maxTimeout;
  private final int revision;
  private final PropagationStrategy propagationStrategy;

  public OpenSessionRequest(
      String member,
      String name,
      String typeName,
      ReadConsistency readConsistency,
      long minTimeout,
      long maxTimeout,
      int revision,
      PropagationStrategy propagationStrategy) {
    this.member = member;
    this.name = name;
    this.typeName = typeName;
    this.readConsistency = readConsistency;
    this.minTimeout = minTimeout;
    this.maxTimeout = maxTimeout;
    this.revision = revision;
    this.propagationStrategy = propagationStrategy;
  }

  /**
   * Returns the client node identifier.
   *
   * @return The client node identifier.
   */
  public String member() {
    return member;
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

  /**
   * Returns the revision number.
   *
   * @return the revision number
   */
  public int revision() {
    return revision;
  }

  /**
   * Returns the revision synchronization strategy.
   *
   * @return the revision synchronization strategy
   */
  public PropagationStrategy propagationStrategy() {
    return propagationStrategy;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), name, typeName, minTimeout, maxTimeout);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof OpenSessionRequest) {
      OpenSessionRequest request = (OpenSessionRequest) object;
      return request.member.equals(member)
          && request.name.equals(name)
          && request.typeName.equals(typeName)
          && request.readConsistency == readConsistency
          && request.minTimeout == minTimeout
          && request.maxTimeout == maxTimeout
          && request.revision == revision
          && request.propagationStrategy == propagationStrategy;
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("node", member)
        .add("serviceName", name)
        .add("serviceType", typeName)
        .add("readConsistency", readConsistency)
        .add("minTimeout", minTimeout)
        .add("maxTimeout", maxTimeout)
        .add("revision", revision)
        .add("synchronizationStrategy", propagationStrategy)
        .toString();
  }

  /**
   * Open session request builder.
   */
  public static class Builder extends AbstractRaftRequest.Builder<Builder, OpenSessionRequest> {
    private String memberId;
    private String serviceName;
    private String serviceType;
    private ReadConsistency readConsistency = ReadConsistency.LINEARIZABLE;
    private long minTimeout;
    private long maxTimeout;
    private int revision;
    private PropagationStrategy propagationStrategy;

    /**
     * Sets the client node identifier.
     *
     * @param member The client node identifier.
     * @return The open session request builder.
     * @throws NullPointerException if {@code node} is {@code null}
     */
    public Builder withMemberId(MemberId member) {
      this.memberId = checkNotNull(member, "node cannot be null").id();
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
     * @param serviceType The service type name.
     * @return The open session request builder.
     * @throws NullPointerException if {@code serviceType} is {@code null}
     */
    public Builder withServiceType(ServiceType serviceType) {
      this.serviceType = checkNotNull(serviceType, "serviceType cannot be null").id();
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

    /**
     * Sets the revision number.
     *
     * @param revision the revision number
     * @return the proxy builder
     */
    public Builder withRevision(int revision) {
      checkArgument(revision >= 0, "revision must be positive");
      this.revision = revision;
      return this;
    }

    /**
     * Sets the revision propagation strategy.
     *
     * @param propagationStrategy the revision propagation strategy
     * @return the proxy builder
     */
    public Builder withPropagationStrategy(PropagationStrategy propagationStrategy) {
      this.propagationStrategy = checkNotNull(propagationStrategy);
      return this;
    }

    @Override
    protected void validate() {
      super.validate();
      checkNotNull(memberId, "client cannot be null");
      checkNotNull(serviceName, "name cannot be null");
      checkNotNull(serviceType, "typeName cannot be null");
      checkArgument(minTimeout >= 0, "minTimeout must be positive");
      checkArgument(maxTimeout >= 0, "maxTimeout must be positive");
      checkArgument(revision >= 0, "revision must be positive");
    }

    /**
     * @throws IllegalStateException is session is not positive
     */
    @Override
    public OpenSessionRequest build() {
      validate();
      return new OpenSessionRequest(
          memberId,
          serviceName,
          serviceType,
          readConsistency,
          minTimeout,
          maxTimeout,
          revision,
          propagationStrategy);
    }
  }
}
