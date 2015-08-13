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

import net.kuujo.copycat.Listener;
import net.kuujo.copycat.ListenerContext;
import net.kuujo.copycat.Resource;
import net.kuujo.copycat.coordination.state.TopicCommands;
import net.kuujo.copycat.coordination.state.TopicState;
import net.kuujo.copycat.raft.server.StateMachine;
import net.kuujo.copycat.resource.ResourceContext;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Async topic.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DistributedTopic<T> extends Resource {
  private final List<TopicListenerContext<T>> listeners = new CopyOnWriteArrayList<>();

  @Override
  protected Class<? extends StateMachine> stateMachine() {
    return TopicState.class;
  }

  @Override
  protected void open(ResourceContext context) {
    super.open(context);
    context.session().onReceive(message -> {
      for (TopicListenerContext<T> listener : listeners) {
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
  public ListenerContext<T> onMessage(Listener<T> listener) {
    TopicListenerContext<T> context = new TopicListenerContext<T>(listener);
    listeners.add(context);
    return context;
  }

  /**
   * Topic listener context.
   */
  private class TopicListenerContext<T> implements ListenerContext<T> {
    private final Listener<T> listener;

    private TopicListenerContext(Listener<T> listener) {
      this.listener = listener;
    }

    @Override
    public void accept(T event) {
      listener.accept(event);
    }

    @Override
    public void close() {
      listeners.remove(this);
    }
  }

}
