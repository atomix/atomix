/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.group.messaging;

import io.atomix.catalyst.util.Assert;

import java.util.concurrent.CompletableFuture;

/**
 * Message producer.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface MessageProducer<T> extends AutoCloseable {

  /**
   * Execution policy.
   */
  enum Execution {
    /**
     * Synchronous execution policy.
     */
    SYNC,

    /**
     * Asynchronous execution policy.
     */
    ASYNC,

    /**
     * Request-reply execution policy.
     */
    REQUEST_REPLY,
  }

  /**
   * Delivery policy.
   */
  enum Delivery {
    /**
     * Delivers a message directly to a specific member.
     */
    DIRECT,

    /**
     * Delivers a message to a random member.
     */
    RANDOM,

    /**
     * Delivers a message to all members of a group.
     */
    BROADCAST,
  }

  /**
   * Task producer options.
   */
  class Options {
    private Delivery delivery = Delivery.BROADCAST;
    private Execution execution = Execution.SYNC;

    /**
     * Sets the producer delivery policy.
     *
     * @param delivery The producer delivery policy.
     * @return The producer options.
     */
    public Options withDelivery(Delivery delivery) {
      this.delivery = Assert.notNull(delivery, "delivery");
      return this;
    }

    /**
     * Returns the producer delivery policy.
     *
     * @return The producer delivery policy.
     */
    public Delivery getDelivery() {
      return delivery;
    }

    /**
     * Sets the producer execution policy.
     *
     * @param execution The producer execution policy.
     * @return The producer options.
     */
    public Options withExecution(Execution execution) {
      this.execution = Assert.notNull(execution, "execution");
      return this;
    }

    /**
     * Returns the producer execution policy.
     *
     * @return The producer execution policy.
     */
    public Execution getExecution() {
      return execution;
    }
  }

  /**
   * Sends a message.
   *
   * @param message The message to send.
   * @return A completable future to be completed once the message has been acknowledged.
   */
  <U> CompletableFuture<U> send(T message);

  @Override
  default void close() {
  }

}
