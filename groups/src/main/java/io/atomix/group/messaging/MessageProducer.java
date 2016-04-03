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
   * Dispatch policy.
   */
  enum DispatchPolicy {
    /**
     * Dispatches a message to a random member.
     */
    RANDOM,

    /**
     * Dispatches a message to all members of a group.
     */
    BROADCAST,
  }

  /**
   * Delivery policy.
   */
  enum DeliveryPolicy {

    /**
     * Synchronous delivery policy.
     */
    SYNC,

    /**
     * Asynchronous delivery policy.
     */
    ASYNC,

    /**
     * Request-reply delivery policy.
     */
    REQUEST_REPLY,
  }

  /**
   * Task producer options.
   */
  class Options {
    private DispatchPolicy dispatchPolicy = DispatchPolicy.BROADCAST;
    private DeliveryPolicy deliveryPolicy = DeliveryPolicy.SYNC;

    /**
     * Sets the producer dispatch policy.
     *
     * @param dispatchPolicy The producer dispatch policy.
     * @return The producer options.
     */
    public Options withDispatchPolicy(DispatchPolicy dispatchPolicy) {
      this.dispatchPolicy = Assert.notNull(dispatchPolicy, "dispatchPolicy");
      return this;
    }

    /**
     * Returns the producer dispatch policy.
     *
     * @return The producer dispatch policy.
     */
    public DispatchPolicy getDispatchPolicy() {
      return dispatchPolicy;
    }

    /**
     * Sets the producer delivery policy.
     *
     * @param deliveryPolicy The producer delivery policy.
     * @return The producer options.
     */
    public Options withDeliveryPolicy(DeliveryPolicy deliveryPolicy) {
      this.deliveryPolicy = Assert.notNull(deliveryPolicy, "deliveryPolicy");
      return this;
    }

    /**
     * Returns the producer delivery policy.
     *
     * @return The producer delivery policy.
     */
    public DeliveryPolicy getDeliveryPolicy() {
      return deliveryPolicy;
    }
  }

  /**
   * Sends a message.
   *
   * @param message The message to send.
   * @return A completable future to be completed once the message has been acknowledged.
   */
  CompletableFuture<Void> send(T message);

  @Override
  default void close() {
  }

}
