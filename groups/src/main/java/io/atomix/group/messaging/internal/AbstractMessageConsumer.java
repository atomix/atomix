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
package io.atomix.group.messaging.internal;

import io.atomix.catalyst.concurrent.Listener;
import io.atomix.group.messaging.Message;
import io.atomix.group.messaging.MessageConsumer;

import java.util.function.Consumer;

/**
 * Abstract message consumer.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class AbstractMessageConsumer<T> implements MessageConsumer<T> {
  private final String name;
  private final AbstractMessageService service;
  private volatile Listener<Message<T>> listener;

  protected AbstractMessageConsumer(String name, Options options, AbstractMessageService service) {
    this.name = name;
    this.service = service;
    service.consumerService().registry().register(name, this);
  }

  /**
   * Returns the consumer name.
   *
   * @return The consumer name.
   */
  String name() {
    return name;
  }

  @Override
  public Listener<Message<T>> onMessage(Consumer<Message<T>> callback) {
    listener = new ConsumerListener<>(callback);
    return listener;
  }

  /**
   * Called when a message is received.
   *
   * @param message The received message.
   */
  void onMessage(GroupMessage<T> message) {
    Listener<Message<T>> listener = this.listener;
    if (listener != null) {
      listener.accept(message.setConsumerService(service.consumerService()));
    } else {
      message.fail();
    }
  }

  @Override
  public void close() {
    service.consumerService().registry().close(name, this);
  }

  /**
   * Message consumer listener.
   */
  private class ConsumerListener<T> implements Listener<Message<T>> {
    private final Consumer<Message<T>> callback;

    private ConsumerListener(Consumer<Message<T>> callback) {
      this.callback = callback;
    }

    @Override
    public void accept(Message<T> message) {
      callback.accept(message);
    }

    @Override
    public void close() {
      if ((Listener) listener == this) {
        AbstractMessageConsumer.this.close();
      }
    }
  }

}
