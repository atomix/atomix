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
package io.atomix.group.connection;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.util.Listener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Local group connection.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class LocalConnection extends Connection {
  private final Map<String, MessageListenerHolder> messageListeners = new ConcurrentHashMap<>();

  public LocalConnection(String memberId, Address address, ConnectionManager connections) {
    super(memberId, address, connections);
  }

  /**
   * Registers a consumer for messages sent to the local member.
   * <p>
   * The provided message consumer will be called when a message sent to the local member
   * is received for the given {@code topic}.
   *
   * @param topic The message topic.
   * @param consumer The message consumer.
   * @param <T> The message type.
   * @return The message listener.
   */
  @SuppressWarnings("unchecked")
  public <T> Listener<Message<T>> onMessage(String topic, Consumer<Message<T>> consumer) {
    MessageListenerHolder listener = new MessageListenerHolder(consumer);
    messageListeners.put(topic, listener);
    return listener;
  }

  /**
   * Handles a message to the member.
   */
  void onMessage(Message message) {
    MessageListenerHolder listener = messageListeners.get(message.topic());
    if (listener != null) {
      listener.accept(message);
    }
  }

  /**
   * Listener holder.
   */
  @SuppressWarnings("unchecked")
  private class MessageListenerHolder implements Listener {
    private final Consumer consumer;

    private MessageListenerHolder(Consumer consumer) {
      this.consumer = consumer;
    }

    @Override
    public void accept(Object message) {
      consumer.accept(message);
    }

    @Override
    public void close() {
      messageListeners.remove(this);
    }
  }

}
