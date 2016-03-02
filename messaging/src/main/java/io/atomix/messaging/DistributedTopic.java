/*
 * Copyright 2015 the original author or authors.
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
package io.atomix.messaging;

import io.atomix.catalyst.util.Listener;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.messaging.state.TopicCommands;
import io.atomix.messaging.util.DistributedTopicFactory;
import io.atomix.resource.AbstractResource;
import io.atomix.resource.ResourceTypeInfo;
import io.atomix.resource.WriteConsistency;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Facilitates persistent publish-subscribe messaging in the cluster.
 * <p>
 * The distributed topic resource provides persistent publish-subscribe messaging between instances
 * of the resource. Pub-sub messaging is implemented as commands that are logged and replicated via the
 * Raft consensus algorithm. When a message is {@link #publish(Object) published} to a distributed topic
 * the message will be persisted until it has been received by all instances {@link #subscribe(Consumer) listening}
 * to the topic.
 * <p>
 * To create a topic resource, use the {@code DistributedTopic} class or constructor:
 * <pre>
 *   {@code
 *   atomix.getTopic("my-topic").thenAccept(topic -> {
 *     ...
 *   });
 *   }
 * </pre>
 * The topic resource exposes two simple methods: {@link #publish(Object)} and {@link #subscribe(Consumer)}.
 * Resources may publish but not subscribe to the topic or subscribe but not publish to the topic. Messages are
 * only routed to the resource instance if a subscribe listener has been registered.
 * <pre>
 *   {@code
 *   topic.subscribe(message -> System.out.println("Received: " + message));
 *   topic.publish("Hello world!");
 *   }
 * </pre>
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@ResourceTypeInfo(id=-31, factory = DistributedTopicFactory.class)
public class DistributedTopic<T> extends AbstractResource<DistributedTopic<T>> {
  private final Set<Consumer<T>> listeners = new HashSet<>();

  @SuppressWarnings("unchecked")
  public DistributedTopic(CopycatClient client, Properties options) {
    super(client, options);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<DistributedTopic<T>> open() {
    return super.open().thenApply(result -> {
      client.onEvent("message", event -> {
        for (Consumer<T> listener : listeners) {
          listener.accept((T) event);
        }
      });
      return result;
    });
  }

  /**
   * Sets the topic to synchronous mode.
   * <p>
   * Setting the topic to synchronous mode effectively configures the topic's {@link WriteConsistency} to
   * {@link WriteConsistency#ATOMIC}. Atomic consistency means that messages {@link #publish(Object) published} to the
   * topic will be received by all {@link #subscribe(Consumer) subscribers} some time between the invocation of
   * the publish operation and its completion.
   *
   * @return The distributed topic.
   */
  public DistributedTopic<T> sync() {
    return with(WriteConsistency.ATOMIC);
  }

  /**
   * Sets the topic to asynchronous mode.
   * <p>
   * Setting the topic to asynchronous mode effectively configures the topic's {@link WriteConsistency} to
   * {@link WriteConsistency#SEQUENTIAL_EVENT}. Sequential consistency means that once a message is {@link #publish(Object) published}
   * to the topic, the message will be persisted in the cluster but may be delivered to {@link #subscribe(Consumer) subscribers}
   * after some arbitrary delay. Messages are guaranteed to be delivered to subscribers in the order in which they were sent
   * (sequential consistency) but different subscribers may receive different messages at different points in time.
   *
   * @return The distributed topic.
   */
  public DistributedTopic<T> async() {
    return with(WriteConsistency.SEQUENTIAL_EVENT);
  }

  /**
   * Publishes a message to the topic.
   * <p>
   * The message will be published according to the {@link #with(WriteConsistency) configured consistency level}.
   * Events published with {@link WriteConsistency#ATOMIC} consistency are guaranteed to be received by all
   * subscribers prior to the {@link CompletableFuture} returned by this method being completed. For all other
   * consistency levels, messages will be received by subscribers asynchronously.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the message has been persisted
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#join()} method to block the calling thread:
   * <pre>
   *   {@code
   *   topic.publish("Hello world!").join();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the lock is acquired in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   topic.publish("Hello world!").thenRun(() -> System.out.println("Published to topic: " + topic.key()));
   *   }
   * </pre>
   *
   * @param message The message to publish.
   * @return A completable future to be completed once the message has been published.
   */
  public CompletableFuture<Void> publish(T message) {
    return submit(new TopicCommands.Publish<>(message));
  }

  /**
   * Subscribes to messages from the topic.
   * <p>
   * Once the returned {@link CompletableFuture} is completed, the subscriber is guaranteed to receive all
   * messages from any client thereafter. Messages are guaranteed to be received in the order specified by
   * the instance from which they were sent. The provided {@link Consumer} will always be executed on the
   * same thread.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the listener has been registered
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#join()} method to block the calling thread:
   * <pre>
   *   {@code
   *   topic.subscribe(message -> {
   *     ...
   *   }).join();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the lock is acquired in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   topic.subscribe(message -> {
   *     ...
   *   }).thenRun(() -> System.out.println("Subscribed to " + topic.key()));
   *   }
   * </pre>
   *
   * @param listener The message listener.
   * @return The listener context.
   */
  public CompletableFuture<Listener<T>> subscribe(Consumer<T> listener) {
    if (!listeners.isEmpty()) {
      listeners.add(listener);
      return CompletableFuture.completedFuture(new TopicListener(listener));
    }

    listeners.add(listener);
    return submit(new TopicCommands.Listen())
      .thenApply(v -> new TopicListener(listener));
  }

  /**
   * Topic listener.
   */
  private class TopicListener implements Listener<T> {
    private final Consumer<T> listener;

    private TopicListener(Consumer<T> listener) {
      this.listener = listener;
    }

    @Override
    public void accept(T message) {
      listener.accept(message);
    }

    @Override
    public void close() {
      synchronized (DistributedTopic.this) {
        listeners.remove(listener);
        if (listeners.isEmpty()) {
          submit(new TopicCommands.Unlisten());
        }
      }
    }
  }

}
