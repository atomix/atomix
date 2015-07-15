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
package net.kuujo.copycat.cluster;

import net.kuujo.copycat.Listener;
import net.kuujo.copycat.ListenerContext;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Cluster topic.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class Topic<T> {
  protected final String name;
  protected final List<ListenerContext<Message<T>>> listeners = new CopyOnWriteArrayList<>();

  public Topic(String name) {
    this.name = name;
  }

  /**
   * Returns the topic name.
   *
   * @return The topic name.
   */
  public String name() {
    return name;
  }

  /**
   * Publishes a message to the topic.
   *
   * @param message The message to publish.
   * @return A completable future to be completed once the message has been published.
   */
  public abstract CompletableFuture<Void> publish(T message);

  /**
   * Sets a message listener on the topic.
   *
   * @param listener The message listener.
   * @return The topic.
   */
  public ListenerContext<Message<T>> onMessage(Listener<Message<T>> listener) {
    ListenerContext<Message<T>> context = new ListenerContext<Message<T>>() {
      @Override
      public void accept(Message<T> message) {
        listener.accept(message);
      }

      @Override
      public void close() {
        if (listeners.remove(this) && listeners.isEmpty()) {
          close();
        }
      }
    };
    listeners.add(context);
    return context;
  }

  /**
   * Closes the topic.
   */
  protected abstract void close();

}
