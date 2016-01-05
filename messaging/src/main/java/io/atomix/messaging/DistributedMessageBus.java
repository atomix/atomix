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

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Client;
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.transport.Server;
import io.atomix.catalyst.util.concurrent.Futures;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.messaging.state.MessageBusCommands;
import io.atomix.messaging.state.MessageBusState;
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceTypeInfo;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Provides a light-weight asynchronous messaging layer over Atomix's {@link io.atomix.catalyst.transport.Transport}.
 * <p>
 * The distributed message bus resource provides a simple interface for asynchronous messaging in an Atomix cluster.
 * Message buses handle management of location information and connections and use Atomix's underlying
 * {@link io.atomix.catalyst.transport.Transport} to communicate across the cluster.
 * <p>
 * To create a message bus resource, use the {@code DistributedMessageBus} class or constructor:
 * <pre>
 *   {@code
 *   atomix.get("bus", DistributedMessageBus.class).thenAccept(bus -> {
 *     ...
 *   });
 *   }
 * </pre>
 * Once a message bus instance has been created, it's not immediately opened. The message bus instance must be explicitly
 * opened by calling {@link #open(Address)}, providing an {@link Address} to which to bind the message bus server. Because
 * each message bus instance runs on a separate server, it's recommended that nodes use a singleton instance of this
 * resource by using {@code get(...)} rather than {@code create(...)} to get a reference to the resource.
 * <p>
 * Messages are produced and consumed by {@link MessageProducer producers} and {@link MessageConsumer consumers} respectively.
 * Each producer and consumer is associated with a string message bus topic.
 * <pre>
 *   {@code
 *   bus.consumer("test", message -> {
 *     return "world!";
 *   });
 *
 *   bus.producer("test").send("Hello");
 *   }
 * </pre>
 * The distributed message bus does <em>not</em> provide reliability guarantees. Messaging is implemented directly on
 * top of the {@link io.atomix.catalyst.transport.Transport} layer with no additional coordination aside from managing
 * a distributed list of topics and their consumers.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@ResourceTypeInfo(id=-30, stateMachine=MessageBusState.class)
public class DistributedMessageBus extends Resource<DistributedMessageBus, Resource.Options> {
  private Client client;
  private Server server;
  private final Map<Integer, Connection> connections = new HashMap<>();
  private volatile CompletableFuture<DistributedMessageBus> openFuture;
  private volatile CompletableFuture<Void> closeFuture;
  private final Map<String, RemoteConsumers> remotes = new ConcurrentHashMap<>();
  private final Map<String, InternalMessageConsumer> consumers = new ConcurrentHashMap<>();
  private volatile boolean open;

  public DistributedMessageBus(CopycatClient client, Resource.Options options) {
    super(client, options);
  }

  /**
   * Opens the message bus.
   * <p>
   * When the message bus is opened, this instance will bind to the provided {@link Address}.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the server is opened
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#join()} method to block the calling thread:
   * <pre>
   *   {@code
   *   bus.open(new Address("123.456.789.0", 5000)).join();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the lock is acquired in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   bus.open(new Address("123.456.789.0", 5000)).thenRun(() -> System.out.println("Message bus open!"));
   *   }
   * </pre>
   *
   * @param address The address on which to listen.
   * @return A completable future to be completed once the message bus is started.
   */
  public synchronized CompletableFuture<DistributedMessageBus> open(Address address) {
    if (openFuture != null)
      return openFuture;

    client = super.client.transport().client();
    server = super.client.transport().server();

    openFuture = new CompletableFuture<>();
    super.client.context().execute(() -> {
      server.listen(address, this::connectListener).whenComplete((result, error) -> {
        synchronized (this) {
          if (error == null) {
            open = true;
            CompletableFuture<DistributedMessageBus> future = openFuture;
            if (future != null) {
              openFuture = null;
              future.complete(null);
            }
          } else {
            open = false;
            CompletableFuture<DistributedMessageBus> future = openFuture;
            if (future != null) {
              openFuture = null;
              future.completeExceptionally(error);
            }
          }
        }
      });
    });

    return openFuture.thenCompose(v -> {
      CompletableFuture<Void> future = new CompletableFuture<>();
      submit(new MessageBusCommands.Join(address)).whenComplete((topics, error) -> {
        if (error == null) {
          for (Map.Entry<String, Set<Address>> entry : topics.entrySet()) {
            remotes.put(entry.getKey(), new RemoteConsumers(entry.getValue()));
          }

          super.client.onEvent("register", this::registerConsumer);
          super.client.onEvent("unregister", this::unregisterConsumer);
          future.complete(null);
        } else {
          future.completeExceptionally(error);
        }
      });
      return future;
    }).thenApply(v -> this);
  }

  /**
   * Returns a boolean value indicating whether the message bus is open.
   *
   * @return Indicates whether the message bus is open.
   */
  public boolean isOpen() {
    return open;
  }

  /**
   * Handles server connections.
   */
  private void connectListener(Connection connection) {
    connection.handler(Message.class, this::handleMessage);
  }

  /**
   * Registers consumer info.
   */
  private void registerConsumer(MessageBusCommands.ConsumerInfo info) {
    RemoteConsumers consumers = remotes.get(info.topic());
    if (consumers == null) {
      consumers = new RemoteConsumers(Collections.singleton(info.address()));
      remotes.put(info.topic(), consumers);
    } else {
      consumers.add(info.address());
    }
  }

  /**
   * Unregisters consumer info.
   */
  private void unregisterConsumer(MessageBusCommands.ConsumerInfo info) {
    RemoteConsumers consumers = remotes.get(info.topic());
    if (consumers != null) {
      if (consumers.remove(info.address())) {
        remotes.remove(info.topic());
      }
    }
  }

  /**
   * Handles a message.
   */
  @SuppressWarnings("unchecked")
  private CompletableFuture<Void> handleMessage(Message message) {
    InternalMessageConsumer consumer = consumers.get(message.topic());
    if (consumer == null) {
      return Futures.exceptionalFuture(new IllegalStateException("unknown topic " + message.topic()));
    }
    return consumer.consume(message.body());
  }

  /**
   * Creates a message producer.
   * <p>
   * The {@code topic} is a cluster-wide identifier. Messages will be distributed to {@link MessageConsumer}s
   * registered for the given {@code topic} in round-robin order.
   * <p>
   * The producer will be created asynchronously. Once the returned {@link CompletableFuture} is
   * completed, the producer will be prepared to send messages.
   *
   * @param topic The topic to which to produce.
   * @param <T> The message type.
   * @return A completable future to be completed once the producer has been created.
   */
  public <T> CompletableFuture<MessageProducer<T>> producer(String topic) {
    return CompletableFuture.completedFuture(new InternalMessageProducer<>(topic));
  }

  /**
   * Creates a message consumer.
   * <p>
   * The consumer will be created asynchronously. Once the consumer has been registered and all other
   * instances of the message bus across the cluster have been notified of the consumer, the returned
   * {@link CompletableFuture} will be completed.
   *
   * @param topic The topic from which to consume.
   * @param <T> The message type.
   * @return A completable future to be completed once the consumer has been registered.
   */
  public <T> CompletableFuture<MessageConsumer<T>> consumer(String topic) {
    return consumer(topic, (Function<T, ?>) null);
  }

  /**
   * Creates a message consumer.
   * <p>
   * The consumer will be created asynchronously. Once the consumer has been registered and all other
   * instances of the message bus across the cluster have been notified of the consumer, the returned
   * {@link CompletableFuture} will be completed.
   *
   * @param topic The topic from which to consume.
   * @param consumer The message consumer.
   * @param <T> The message type.
   * @return A completable future to be completed once the consumer has been registered.
   */
  public <T> CompletableFuture<MessageConsumer<T>> consumer(String topic, Function<T, ?> consumer) {
    CompletableFuture<MessageConsumer<T>> future = new CompletableFuture<>();
    submit(new MessageBusCommands.Register(topic)).whenComplete((result, error) -> {
      if (error == null) {
        InternalMessageConsumer<T> internalConsumer = new InternalMessageConsumer<>(topic, consumer);
        this.consumers.put(topic, internalConsumer);
        future.complete(internalConsumer);
      } else {
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  /**
   * Returns a connection to the next consumer for the topic.
   */
  private CompletableFuture<Connection> next(String topic) {
    RemoteConsumers consumers = remotes.get(topic);
    if (consumers == null)
      return CompletableFuture.completedFuture(null);

    Address next = consumers.next();
    if (next == null)
      return CompletableFuture.completedFuture(null);
    return getConnection(next);
  }

  /**
   * Returns the connection for the given member.
   *
   * @param address The member for which to get the connection.
   * @return A completable future to be called once the connection is received.
   */
  private CompletableFuture<Connection> getConnection(Address address) {
    Connection connection = connections.get(address.hashCode());
    return connection == null ? createConnection(address) : CompletableFuture.completedFuture(connection);
  }

  /**
   * Creates a connection for the given member.
   *
   * @param address The member for which to create the connection.
   * @return A completable future to be called once the connection has been created.
   */
  private CompletableFuture<Connection> createConnection(Address address) {
    return client.connect(address).thenApply(connection -> {
      connections.put(address.hashCode(), connection);
      connection.closeListener(c -> connections.remove(address.hashCode()));
      return connection;
    });
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    if (closeFuture != null)
      return closeFuture;

    if (server == null)
      return Futures.exceptionalFuture(new IllegalStateException("message bus not open"));

    closeFuture = new CompletableFuture<>();
    super.client.context().execute(() -> {
      server.close().whenComplete((result, error) -> {
        synchronized (this) {
          open = false;
          if (error == null) {
            CompletableFuture<Void> future = closeFuture;
            if (future != null) {
              closeFuture = null;
              future.complete(null);
            }
          } else {
            open = false;
            CompletableFuture<Void> future = closeFuture;
            if (future != null) {
              closeFuture = null;
              future.completeExceptionally(error);
            }
          }
        }
      });
    });
    return closeFuture;
  }

  /**
   * Returns a boolean value indicating whether the message bus is closed.
   *
   * @return Indicates whether the message bus is closed.
   */
  public boolean isClosed() {
    return !open;
  }

  /**
   * Internal message consumer.
   */
  private class InternalMessageConsumer<T> implements MessageConsumer<T> {
    private final String topic;
    private Function<T, ?> consumer;

    private InternalMessageConsumer(String topic, Function<T, ?> consumer) {
      this.topic = topic;
      this.consumer = consumer;
    }

    @Override
    public String topic() {
      return topic;
    }

    @Override
    public MessageConsumer<T> onMessage(Function<T, ?> consumer) {
      this.consumer = consumer;
      return this;
    }

    @SuppressWarnings("unchecked")
    private CompletableFuture<Object> consume(Object message) {
      Object result = consumer.apply((T) message);
      if (result instanceof CompletableFuture) {
        return (CompletableFuture) result;
      }
      return CompletableFuture.completedFuture(result);
    }

    @Override
    public CompletableFuture<Void> close() {
      return submit(new MessageBusCommands.Unregister(topic));
    }
  }

  /**
   * Internal message producer.
   */
  private class InternalMessageProducer<T> implements MessageProducer<T> {
    private final String topic;

    private InternalMessageProducer(String topic) {
      this.topic = topic;
    }

    @Override
    public String topic() {
      return topic;
    }

    @Override
    public <U> CompletableFuture<U> send(T message) {
      return next(topic).thenCompose(c -> {
        if (c == null)
          return Futures.exceptionalFuture(new IllegalStateException("no handlers"));
        return c.send(new Message(topic, message));
      });
    }

    @Override
    public CompletableFuture<Void> close() {
      return CompletableFuture.completedFuture(null);
    }
  }

  /**
   * Remote consumers holder.
   */
  private static class RemoteConsumers {
    private final List<Address> consumers;
    private Iterator<Address> iterator;

    private RemoteConsumers(Set<Address> consumers) {
      this.consumers = new ArrayList<>(consumers);
    }

    /**
     * Adds a consumer to the list.
     */
    private void add(Address address) {
      consumers.add(address);
      iterator = consumers.iterator();
    }

    /**
     * Removes a consumer from the list.
     */
    private boolean remove(Address address) {
      consumers.remove(address);
      return consumers.isEmpty();
    }

    /**
     * Returns the next address for the consumers.
     */
    private Address next() {
      if (consumers.isEmpty())
        return null;
      if (iterator == null || !iterator.hasNext())
        iterator = consumers.iterator();
      return iterator.next();
    }
  }

}
