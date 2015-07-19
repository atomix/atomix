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
package net.kuujo.copycat.queue;

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.AlleycatSerializable;
import net.kuujo.alleycat.io.BufferInput;
import net.kuujo.alleycat.io.BufferOutput;
import net.kuujo.copycat.AbstractResource;
import net.kuujo.copycat.Listener;
import net.kuujo.copycat.ListenerContext;
import net.kuujo.copycat.raft.Command;
import net.kuujo.copycat.raft.Operation;
import net.kuujo.copycat.raft.Raft;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Async topic.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AsyncTopic<T> extends AbstractResource {
  private final List<TopicListenerContext<T>> listeners = new CopyOnWriteArrayList<>();

  @SuppressWarnings("unchecked")
  public AsyncTopic(Raft protocol) {
    super(protocol);
    protocol.session().onReceive(message -> {
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
    return submit(PublishCommand.builder()
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

  /**
   * Abstract topic command.
   */
  public static abstract class TopicCommand<V> implements Command<V>, AlleycatSerializable {

    /**
     * Base map command builder.
     */
    public static abstract class Builder<T extends Builder<T, U>, U extends TopicCommand<?>> extends Command.Builder<T, U> {
    }
  }

  /**
   * Publish command.
   */
  public static class PublishCommand<T> extends TopicCommand<Void> {

    /**
     * Returns a new publish command builder.
     *
     * @param <T> The message type.
     * @return The publish command builder.
     */
    @SuppressWarnings("unchecked")
    public static <T> Builder<T> builder() {
      return Operation.builder(Builder.class);
    }

    private T message;

    @Override
    public void writeObject(BufferOutput buffer, Alleycat serializer) {
      serializer.writeObject(message, buffer);
    }

    @Override
    public void readObject(BufferInput buffer, Alleycat serializer) {
      message = serializer.readObject(buffer);
    }

    /**
     * Publish command builder.
     */
    public static class Builder<T> extends TopicCommand.Builder<Builder<T>, PublishCommand<T>> {

      /**
       * Sets the publish command message.
       *
       * @param message The message.
       * @return The publish command builder.
       */
      public Builder<T> withMessage(T message) {
        command.message = message;
        return this;
      }

      @Override
      protected PublishCommand<T> create() {
        return new PublishCommand<>();
      }
    }
  }

  /**
   * Subscribe command.
   */
  public static class SubscribeCommand extends TopicCommand<Void> {

    /**
     * Returns a new publish command builder.
     *
     * @return The publish command builder.
     */
    @SuppressWarnings("unchecked")
    public static Builder builder() {
      return Operation.builder(Builder.class);
    }

    @Override
    public void writeObject(BufferOutput buffer, Alleycat alleycat) {

    }

    @Override
    public void readObject(BufferInput buffer, Alleycat alleycat) {

    }

    /**
     * Publish command builder.
     */
    public static class Builder extends TopicCommand.Builder<Builder, SubscribeCommand> {
      @Override
      protected SubscribeCommand create() {
        return new SubscribeCommand();
      }
    }
  }

}
