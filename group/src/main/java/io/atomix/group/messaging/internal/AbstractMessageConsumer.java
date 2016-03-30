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

import io.atomix.catalyst.util.Listener;
import io.atomix.group.messaging.Message;
import io.atomix.group.messaging.MessageConsumer;

import java.util.function.Consumer;

/**
 * Abstract message consumer.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class AbstractMessageConsumer<T> implements MessageConsumer<T> {
  protected final String name;
  protected volatile Options options;
  protected final AbstractMessageService service;
  private volatile Listener<Message<T>> listener;

  public AbstractMessageConsumer(String name, Options options, AbstractMessageService service) {
    this.name = name;
    this.options = options;
    this.service = service;
  }

  /**
   * Returns the consumer name.
   *
   * @return The consumer name.
   */
  String name() {
    return name;
  }

  /**
   * Sets the producer options.
   */
  void setOptions(Options options) {
    this.options = options;
  }

  @Override
  public Listener<Message<T>> onMessage(Consumer<Message<T>> callback) {
    listener = new ConsumerListener<>(callback);
    return listener;
  }

  void onMessage(Message<T> message) {
    listener.accept(message);
  }

  @Override
  public void close() {
    service.close(this);
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
