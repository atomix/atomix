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

import io.atomix.group.messaging.MessageProducer;

/**
 * Abstract message producer.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class AbstractMessageProducer<T> implements MessageProducer<T> {
  protected final String name;
  protected volatile MessageProducer.Options options;
  protected final AbstractMessageClient client;

  protected AbstractMessageProducer(String name, MessageProducer.Options options, AbstractMessageClient client) {
    this.name = name;
    this.options = options;
    this.client = client;
  }

  /**
   * Returns the producer name.
   *
   * @return The producer name.
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
  public void close() {
    client.close(this);
  }

}
