/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.primitives.queue;

import com.google.common.util.concurrent.MoreExecutors;
import io.atomix.primitives.DistributedPrimitive;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * A distributed publish subscribe primitive.
 * <p>
 * This primitive provides ordered message delivery guarantee i.e. all messages will be delivered to
 * all <i>active</i> subscribers and messages published from each publisher will be delivered
 * to all active subscribers in the order in which they are published.
 * <p>
 * Transient disruptions in communication such as occasional message drops are automatically handled
 * and recovered from without loss of delivery guarantees.
 * <p>
 * However, subscribers need to remain active or alive for these guarantees to apply. A subscriber that is
 * partitioned away for an extended duration (typically 5 seconds or more) will be marked as inactive and
 * during that period of inactivity will be removed from the list of current subscribers.
 *
 * @param <T> The type of message to be distributed to subscribers
 */
public interface Topic<T> extends DistributedPrimitive {

  /**
   * Publishes a message to all subscribers.
   * <p>
   * The message is delivered in a asynchronous fashion which means subscribers will receive the
   * message eventually but not necessarily before the future returned by this method is completed.
   *
   * @param message The non-null message to send to all current subscribers
   * @return a future that is completed when the message is logged (not necessarily delivered).
   */
  CompletableFuture<Void> publish(T message);

  /**
   * Subscribes to messages published to this topic.
   *
   * @param callback callback that will invoked when a message published to the topic is received.
   * @param executor executor for running the callback
   * @return a future that is completed when subscription request is completed.
   */
  CompletableFuture<Void> subscribe(Consumer<T> callback, Executor executor);

  /**
   * Subscribes to messages published to this topic.
   *
   * @param callback callback that will invoked when a message published to the topic is received.
   * @return a future that is completed when subscription request is completed.
   */
  default CompletableFuture<Void> subscribe(Consumer<T> callback) {
    return subscribe(callback, MoreExecutors.directExecutor());
  }

  /**
   * Unsubscribes from this topic.
   *
   * @param callback previously subscribed callback
   * @return a future that is completed when unsubscription request is completed.
   */
  CompletableFuture<Void> unsubscribe(Consumer<T> callback);
}