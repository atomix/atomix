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
package net.kuujo.copycat.coordination;

import net.kuujo.copycat.Resource;
import net.kuujo.copycat.coordination.state.TopicCommands;
import net.kuujo.copycat.coordination.state.TopicState;
import net.kuujo.copycat.raft.StateMachine;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.util.Listener;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Async topic.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DistributedTopic<T> extends Resource {
  private final Set<Consumer<T>> listeners = new HashSet<>();

  @Override
  protected Class<? extends StateMachine> stateMachine() {
    return TopicState.class;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected void open(ResourceContext context) {
    super.open(context);
    context.session().onReceive(message -> {
      for (Consumer<T> listener : listeners) {
        listener.accept((T) message);
      }
    });
  }

  /**
   * Publishes a message to the topic.
   *
   * @param message The message to publish.
   * @return A completable future to be completed once the message has been published.
   */
  public CompletableFuture<Void> publish(T message) {
    return submit(TopicCommands.Publish.builder()
      .withMessage(message)
      .build());
  }

  /**
   * Sets a message listener on the topic.
   *
   * @param listener The message listener.
   * @return The listener context.
   */
  public CompletableFuture<Listener<T>> onMessage(Consumer<T> listener) {
    if (!listeners.isEmpty()) {
      listeners.add(listener);
      return CompletableFuture.completedFuture(new TopicListener(listener));
    }

    listeners.add(listener);
    return submit(TopicCommands.Listen.builder().build())
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
          submit(TopicCommands.Unlisten.builder().build());
        }
      }
    }
  }

}
