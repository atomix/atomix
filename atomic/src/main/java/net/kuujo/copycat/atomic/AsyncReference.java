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
package net.kuujo.copycat.atomic;

import net.kuujo.copycat.AbstractResource;
import net.kuujo.copycat.Stateful;
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.serializer.Writable;
import net.kuujo.copycat.raft.*;
import net.kuujo.copycat.raft.log.Compaction;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Asynchronous atomic value.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Stateful(AsyncReference.StateMachine.class)
public class AsyncReference<T> extends AbstractResource {

  public AsyncReference(Protocol protocol) {
    super(protocol);
  }

  /**
   * Gets the current value.
   *
   * @return A completable future to be completed with the current value.
   */
  public CompletableFuture<T> get() {
    return submit(Get.<T>builder().build());
  }

  /**
   * Sets the current value.
   *
   * @param value The current value.
   * @return A completable future to be completed once the value has been set.
   */
  public CompletableFuture<Void> set(T value) {
    return submit(Set.builder().withValue(value).build());
  }

  /**
   * Gets the current value and updates it.
   *
   * @param value The updated value.
   * @return A completable future to be completed with the previous value.
   */
  public CompletableFuture<T> getAndSet(T value) {
    return submit(GetAndSet.<T>builder().withValue(value).build());
  }

  /**
   * Compares the current value and updated it if expected value == the current value.
   *
   * @param expect The expected value.
   * @param update The updated value.
   * @return A completable future to be completed with a boolean value indicating whether the value was updated.
   */
  public CompletableFuture<Boolean> compareAndSet(T expect, T update) {
    return submit(CompareAndSet.builder().withExpect(expect).withUpdate(update).build());
  }

  /**
   * Abstract reference command.
   */
  public static abstract class ReferenceCommand<V> implements Command<V>, Writable {

    @Override
    public void writeObject(Buffer buffer, Serializer serializer) {

    }

    @Override
    public void readObject(Buffer buffer, Serializer serializer) {

    }

    /**
     * Base reference command builder.
     */
    public static abstract class Builder<T extends Builder<T, U>, U extends ReferenceCommand<?>> extends Command.Builder<T, U> {
    }
  }

  /**
   * Abstract reference query.
   */
  public static abstract class ReferenceQuery<V> implements Query<V>, Writable {

    @Override
    public ConsistencyLevel consistency() {
      return ConsistencyLevel.LINEARIZABLE_STRICT;
    }

    @Override
    public void writeObject(Buffer buffer, Serializer serializer) {

    }

    @Override
    public void readObject(Buffer buffer, Serializer serializer) {

    }

    /**
     * Base reference query builder.
     */
    public static abstract class Builder<T extends Builder<T, U>, U extends ReferenceQuery<?>> extends Query.Builder<T, U> {
    }
  }

  /**
   * Get query.
   */
  public static class Get<T> extends ReferenceQuery<T> {

    /**
     * Returns a new get query builder.
     *
     * @return A new get query builder.
     */
    @SuppressWarnings("unchecked")
    public static <T> Builder<T> builder() {
      return Operation.builder(Builder.class);
    }

    /**
     * Get query builder.
     */
    public static class Builder<T> extends ReferenceQuery.Builder<Builder<T>, Get<T>> {
      @Override
      protected Get<T> create() {
        return new Get<>();
      }
    }
  }

  /**
   * Set command.
   */
  public static class Set extends ReferenceCommand<Void> {

    /**
     * Returns a new set command builder.
     *
     * @return A new set command builder.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class);
    }

    private Object value;

    /**
     * Returns the command value.
     *
     * @return The command value.
     */
    public Object value() {
      return value;
    }

    @Override
    public void writeObject(Buffer buffer, Serializer serializer) {
      serializer.writeObject(value, buffer);
    }

    @Override
    public void readObject(Buffer buffer, Serializer serializer) {
      value = serializer.readObject(buffer);
    }

    /**
     * Put command builder.
     */
    public static class Builder extends ReferenceCommand.Builder<Builder, Set> {
      @Override
      protected Set create() {
        return new Set();
      }

      /**
       * Sets the command value.
       *
       * @param value The command value.
       * @return The command builder.
       */
      public Builder withValue(Object value) {
        command.value = value;
        return this;
      }
    }
  }

  /**
   * Compare and set command.
   */
  public static class CompareAndSet extends ReferenceCommand<Boolean> {

    /**
     * Returns a new compare and set command builder.
     *
     * @return A new compare and set command builder.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class);
    }

    private Object expect;
    private Object update;

    /**
     * Returns the expected value.
     *
     * @return The expected value.
     */
    public Object expect() {
      return expect;
    }

    /**
     * Returns the updated value.
     *
     * @return The updated value.
     */
    public Object update() {
      return update;
    }

    @Override
    public void writeObject(Buffer buffer, Serializer serializer) {
      serializer.writeObject(expect, buffer);
      serializer.writeObject(update, buffer);
    }

    @Override
    public void readObject(Buffer buffer, Serializer serializer) {
      expect = serializer.readObject(buffer);
      update = serializer.readObject(buffer);
    }

    /**
     * Compare and set command builder.
     */
    public static class Builder extends ReferenceCommand.Builder<Builder, CompareAndSet> {
      @Override
      protected CompareAndSet create() {
        return new CompareAndSet();
      }

      /**
       * Sets the expected value.
       *
       * @param expect The expected value.
       * @return The command builder.
       */
      public Builder withExpect(Object expect) {
        command.expect = expect;
        return this;
      }

      /**
       * Sets the updated value.
       *
       * @param update The updated value.
       * @return The command builder.
       */
      public Builder withUpdate(Object update) {
        command.update = update;
        return this;
      }
    }
  }

  /**
   * Get and set command.
   */
  public static class GetAndSet<T> extends ReferenceCommand<T> {

    /**
     * Returns a new get and set command builder.
     *
     * @return A new get and set command builder.
     */
    @SuppressWarnings("unchecked")
    public static <T> Builder<T> builder() {
      return Operation.builder(Builder.class);
    }

    private Object value;

    /**
     * Returns the command value.
     *
     * @return The command value.
     */
    public Object value() {
      return value;
    }

    @Override
    public void writeObject(Buffer buffer, Serializer serializer) {
      serializer.writeObject(value, buffer);
    }

    @Override
    public void readObject(Buffer buffer, Serializer serializer) {
      value = serializer.readObject(buffer);
    }

    /**
     * Put command builder.
     */
    public static class Builder<T> extends ReferenceCommand.Builder<Builder<T>, GetAndSet<T>> {
      @Override
      protected GetAndSet<T> create() {
        return new GetAndSet<>();
      }

      /**
       * Sets the command value.
       *
       * @param value The command value.
       * @return The command builder.
       */
      public Builder<T> withValue(Object value) {
        command.value = value;
        return this;
      }
    }
  }

  /**
   * Async reference state machine.
   */
  public static class StateMachine extends net.kuujo.copycat.raft.StateMachine {
    private final AtomicReference<Object> value = new AtomicReference<>();
    private long version;

    /**
     * Handles a get commit.
     */
    @Apply(Get.class)
    protected Object get(Commit<Get> commit) {
      return value.get();
    }

    /**
     * Applies a set commit.
     */
    @Apply(Set.class)
    protected void set(Commit<Set> commit) {
      value.set(commit.operation().value());
      version = commit.index();
    }

    /**
     * Handles a compare and set commit.
     */
    @Apply(CompareAndSet.class)
    protected boolean compareAndSet(Commit<CompareAndSet> commit) {
      return value.compareAndSet(commit.operation().expect(), commit.operation().update());
    }

    /**
     * Handles a get and set commit.
     */
    @Apply(GetAndSet.class)
    protected Object getAndSet(Commit<GetAndSet> commit) {
      Object result = value.getAndSet(commit.operation().value());
      version = commit.index();
      return result;
    }

    /**
     * Filters all entries.
     */
    @Filter(Filter.All.class)
    protected boolean filterAll(Commit<? extends ReferenceCommand<?>> commit, Compaction compaction) {
      return commit.index() >= version;
    }
  }

}
