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
package io.atomix.coordination;

import io.atomix.Resource;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Client;
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.transport.Server;
import io.atomix.catalyst.util.concurrent.Futures;
import io.atomix.coordination.state.MessageBusCommands;
import io.atomix.coordination.state.MessageBusState;
import io.atomix.copycat.server.StateMachine;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Distributed message bus.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class DistributedMessageBus extends Resource<DistributedMessageBus> {
  private final UUID id = UUID.randomUUID();
  private Client client;
  private Server server;
  private final Map<Integer, Connection> connections = new HashMap<>();
  private volatile CompletableFuture<DistributedMessageBus> openFuture;
  private volatile CompletableFuture<Void> closeFuture;
  private final Map<String, RemoteConsumers> remotes = new ConcurrentHashMap<>();
  private final Map<String, InternalMessageConsumer> consumers = new ConcurrentHashMap<>();
  private volatile boolean open;

  @Override
  protected Class<? extends StateMachine> stateMachine() {
    return MessageBusState.class;
  }

  /**
   * Opens the message bus.
   *
   * @param address The address on which to listen.
   * @return A completable future to be completed once the message bus is started.
   */
  public synchronized CompletableFuture<DistributedMessageBus> open(Address address) {
    if (openFuture != null)
      return openFuture;

    client = context.transport().client();
    server = context.transport().server();

    openFuture = new CompletableFuture<>();
    context.context().execute(() -> {
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
      submit(MessageBusCommands.Join.builder()
        .withMember(address)
        .build()).whenComplete((topics, error) -> {
        if (error == null) {
          for (Map.Entry<String, Set<Address>> entry : topics.entrySet()) {
            remotes.put(entry.getKey(), new RemoteConsumers(entry.getValue()));
          }

          context.session().onEvent("register", this::registerConsumer);
          context.session().onEvent("unregister", this::unregisterConsumer);
          future.complete(null);
        } else {
          future.completeExceptionally(error);
        }
      });
      return future;
    }).thenApply(v -> this);
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
   *
   * @param topic The topic from which to consume.
   * @param consumer The message consumer.
   * @param <T> The message type.
   * @return A completable future to be completed once the consumer has been registered.
   */
  public <T> CompletableFuture<MessageConsumer<T>> consumer(String topic, Function<T, ?> consumer) {
    CompletableFuture<MessageConsumer<T>> future = new CompletableFuture<>();
    submit(MessageBusCommands.Register.builder()
      .withTopic(topic)
      .build()).whenComplete((result, error) -> {
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

  /**
   * Closes the message bus.
   *
   * @return A completable future to be completed once the message bus is closed.
   */
  public synchronized CompletableFuture<Void> close() {
    if (closeFuture != null)
      return closeFuture;

    if (server == null)
      return Futures.exceptionalFuture(new IllegalStateException("message bus not open"));

    closeFuture = new CompletableFuture<>();
    context.context().execute(() -> {
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
      return submit(MessageBusCommands.Unregister.builder()
        .withTopic(topic)
        .build());
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
