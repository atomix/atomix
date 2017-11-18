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

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.protocols.raft.ReadConsistency;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft primitive proxy.
 */
public interface RaftProxy extends PrimitiveProxy {

  /**
   * Raft primitive proxy builder.
   */
  abstract class Builder extends PrimitiveProxy.Builder {
    protected ReadConsistency readConsistency = ReadConsistency.LINEARIZABLE;
    protected CommunicationStrategy communicationStrategy = CommunicationStrategy.LEADER;
    protected RecoveryStrategy recoveryStrategy = RecoveryStrategy.RECOVER;

    protected Builder(String name, PrimitiveType primitiveType) {
      super(name, primitiveType);
    }

    @Override
    public Builder withMaxRetries(int maxRetries) {
      return (Builder) super.withMaxRetries(maxRetries);
    }

    @Override
    public Builder withRetryDelayMillis(long retryDelayMillis) {
      return (Builder) super.withRetryDelayMillis(retryDelayMillis);
    }

    @Override
    public Builder withRetryDelay(long retryDelay, TimeUnit timeUnit) {
      return (Builder) super.withRetryDelay(retryDelay, timeUnit);
    }

    @Override
    public Builder withRetryDelay(Duration retryDelay) {
      return (Builder) super.withRetryDelay(retryDelay);
    }

    @Override
    public Builder withMinTimeout(long timeoutMillis) {
      return (Builder) super.withMinTimeout(timeoutMillis);
    }

    @Override
    public Builder withMinTimeout(Duration timeout) {
      return (Builder) super.withMinTimeout(timeout);
    }

    @Override
    public Builder withTimeout(long timeoutMillis) {
      return (Builder) super.withTimeout(timeoutMillis);
    }

    @Override
    public Builder withTimeout(Duration timeout) {
      return (Builder) super.withTimeout(timeout);
    }

    @Override
    public Builder withMaxTimeout(long timeoutMillis) {
      return (Builder) super.withMaxTimeout(timeoutMillis);
    }

    @Override
    public Builder withMaxTimeout(Duration timeout) {
      return (Builder) super.withMaxTimeout(timeout);
    }

    @Override
    public Builder withExecutor(Executor executor) {
      return (Builder) super.withExecutor(executor);
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
  }
}
