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
package io.atomix.protocols.raft.proxy;

import io.atomix.protocols.raft.RaftException;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.service.ServiceType;
import io.atomix.protocols.raft.service.PropagationStrategy;
import io.atomix.utils.Managed;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft proxy client.
 */
public interface RaftProxyClient extends RaftProxyExecutor, Managed<RaftProxyClient> {

  /**
   * Raft session builder.
   */
  abstract class Builder implements io.atomix.utils.Builder<RaftProxyClient> {
    protected String name;
    protected ServiceType serviceType;
    protected ReadConsistency readConsistency = ReadConsistency.LINEARIZABLE;
    protected int maxRetries = 0;
    protected Duration retryDelay = Duration.ofMillis(100);
    protected Executor executor;
    protected CommunicationStrategy communicationStrategy = CommunicationStrategy.LEADER;
    protected RecoveryStrategy recoveryStrategy = RecoveryStrategy.RECOVER;
    protected Duration minTimeout = Duration.ofMillis(250);
    protected Duration maxTimeout = Duration.ofMillis(0);
    protected int revision = 0;
    protected PropagationStrategy propagationStrategy = PropagationStrategy.NONE;

    /**
     * Sets the session name.
     *
     * @param name The service name.
     * @return The session builder.
     */
    public Builder withName(String name) {
      this.name = checkNotNull(name, "name cannot be null");
      return this;
    }

    /**
     * Sets the service type.
     *
     * @param serviceType The service type.
     * @return The session builder.
     */
    public Builder withServiceType(String serviceType) {
      return withServiceType(ServiceType.from(serviceType));
    }

    /**
     * Sets the service type.
     *
     * @param serviceType The service type.
     * @return The session builder.
     */
    public Builder withServiceType(ServiceType serviceType) {
      this.serviceType = checkNotNull(serviceType, "serviceType cannot be null");
      return this;
    }

    /**
     * Sets the session's read consistency level.
     *
     * @param consistency the session's read consistency level
     * @return the proxy builder
     */
    public Builder withReadConsistency(ReadConsistency consistency) {
      this.readConsistency = checkNotNull(consistency, "consistency cannot be null");
      return this;
    }

    /**
     * Sets the session's communication strategy.
     *
     * @param communicationStrategy The session's communication strategy.
     * @return The session builder.
     * @throws NullPointerException if the communication strategy is null
     */
    public Builder withCommunicationStrategy(CommunicationStrategy communicationStrategy) {
      this.communicationStrategy = checkNotNull(communicationStrategy, "communicationStrategy");
      return this;
    }

    /**
     * Sets the maximum number of retries before an operation can be failed.
     *
     * @param maxRetries the maximum number of retries before an operation can be failed
     * @return the proxy builder
     */
    public Builder withMaxRetries(int maxRetries) {
      checkArgument(maxRetries >= 0, "maxRetries must be positive");
      this.maxRetries = maxRetries;
      return this;
    }

    /**
     * Sets the operation retry delay.
     *
     * @param retryDelayMillis the delay between operation retries in milliseconds
     * @return the proxy builder
     */
    public Builder withRetryDelayMillis(long retryDelayMillis) {
      return withRetryDelay(Duration.ofMillis(retryDelayMillis));
    }

    /**
     * Sets the operation retry delay.
     *
     * @param retryDelay the delay between operation retries
     * @param timeUnit the delay time unit
     * @return the proxy builder
     * @throws NullPointerException if the time unit is null
     */
    public Builder withRetryDelay(long retryDelay, TimeUnit timeUnit) {
      return withRetryDelay(Duration.ofMillis(timeUnit.toMillis(retryDelay)));
    }

    /**
     * Sets the operation retry delay.
     *
     * @param retryDelay the delay between operation retries
     * @return the proxy builder
     * @throws NullPointerException if the delay is null
     */
    public Builder withRetryDelay(Duration retryDelay) {
      this.retryDelay = checkNotNull(retryDelay, "retryDelay cannot be null");
      return this;
    }

    /**
     * Sets the session recovery strategy.
     *
     * @param recoveryStrategy the session recovery strategy
     * @return the proxy builder
     * @throws NullPointerException if the strategy is null
     */
    public Builder withRecoveryStrategy(RecoveryStrategy recoveryStrategy) {
      this.recoveryStrategy = checkNotNull(recoveryStrategy, "recoveryStrategy cannot be null");
      return this;
    }

    /**
     * Sets the session timeout.
     *
     * @param timeoutMillis The session timeout.
     * @return The session builder.
     * @throws IllegalArgumentException if the session timeout is not positive
     */
    public Builder withMinTimeout(long timeoutMillis) {
      return withMinTimeout(Duration.ofMillis(timeoutMillis));
    }

    /**
     * Sets the session timeout.
     *
     * @param timeout The session timeout.
     * @return The session builder.
     * @throws IllegalArgumentException if the session timeout is not positive
     * @throws NullPointerException     if the timeout is null
     */
    public Builder withMinTimeout(Duration timeout) {
      checkArgument(!checkNotNull(timeout).isNegative(), "timeout must be positive");
      this.minTimeout = timeout;
      return this;
    }

    /**
     * Sets the session timeout.
     *
     * @param timeoutMillis The session timeout.
     * @return The session builder.
     * @throws IllegalArgumentException if the session timeout is not positive
     */
    @Deprecated
    public Builder withTimeout(long timeoutMillis) {
      return withMaxTimeout(Duration.ofMillis(timeoutMillis));
    }

    /**
     * Sets the session timeout.
     *
     * @param timeout The session timeout.
     * @return The session builder.
     * @throws IllegalArgumentException if the session timeout is not positive
     * @throws NullPointerException     if the timeout is null
     */
    @Deprecated
    public Builder withTimeout(Duration timeout) {
      return withMaxTimeout(timeout);
    }

    /**
     * Sets the session timeout.
     *
     * @param timeoutMillis The session timeout.
     * @return The session builder.
     * @throws IllegalArgumentException if the session timeout is not positive
     */
    public Builder withMaxTimeout(long timeoutMillis) {
      return withMaxTimeout(Duration.ofMillis(timeoutMillis));
    }

    /**
     * Sets the session timeout.
     *
     * @param timeout The session timeout.
     * @return The session builder.
     * @throws IllegalArgumentException if the session timeout is not positive
     * @throws NullPointerException     if the timeout is null
     */
    public Builder withMaxTimeout(Duration timeout) {
      checkArgument(!checkNotNull(timeout).isNegative(), "timeout must be positive");
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

    /**
     * Builds the proxy client.
     *
     * @return the proxy client
     */
    @Override
    public RaftProxyClient build() {
      try {
        return buildAsync().join();
      } catch (CompletionException e) {
        if (e.getCause() instanceof RaftException.Unavailable) {
          throw (RaftException.Unavailable) e.getCause();
        } else {
          throw new RaftException.Unavailable(e);
        }
      }
    }

    /**
     * Returns a future to be completed once the proxy client has been connected.
     *
     * @return a future to be completed once the proxy client has been connected
     */
    public abstract CompletableFuture<RaftProxyClient> buildAsync();
  }
}
