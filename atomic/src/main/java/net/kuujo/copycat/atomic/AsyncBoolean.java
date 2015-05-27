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

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.serializer.Writable;
import net.kuujo.copycat.raft.*;
import net.kuujo.copycat.raft.storage.compact.Compaction;
import net.kuujo.copycat.resource.AbstractResource;
import net.kuujo.copycat.resource.Stateful;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Asynchronous atomic boolean.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Stateful(AsyncBoolean.StateMachine.class)
public class AsyncBoolean extends AbstractResource {

  public AsyncBoolean(Protocol protocol) {
    super(protocol);
  }

  /**
   * Gets the current value.
   *
   * @return A completable future to be completed with the current value.
   */
  public CompletableFuture<Boolean> get() {
    return submit(Get.builder().build());
  }

  /**
   * Sets the current value.
   *
   * @param value The current value.
   * @return A completable future to be completed once the value has been set.
   */
  public CompletableFuture<Void> set(boolean value) {
    return submit(Set.builder().withValue(value).build());
  }

  /**
   * Gets the current value and updates it.
   *
   * @param value The updated value.
   * @return A completable future to be completed with the previous value.
   */
  public CompletableFuture<Boolean> getAndSet(boolean value) {
    return submit(GetAndSet.builder().withValue(value).build());
  }

  /**
   * Compares the current value and updated it if expected value == the current value.
   *
   * @param expect The expected value.
   * @param update The updated value.
   * @return A completable future to be completed with a boolean value indicating whether the value was updated.
   */
  public CompletableFuture<Boolean> compareAndSet(boolean expect, boolean update) {
    return submit(CompareAndSet.builder().withExpect(expect).withUpdate(update).build());
  }

  /**
   * Abstract boolean command.
   */
  public static abstract class BooleanCommand<V> implements Command<V>, Writable {

    @Override
    public void writeObject(Buffer buffer, Serializer serializer) {

    }

    @Override
    public void readObject(Buffer buffer, Serializer serializer) {

    }

    /**
     * Base boolean command builder.
     */
    public static abstract class Builder<T extends Builder<T, U>, U extends BooleanCommand<?>> extends Command.Builder<T, U> {
      protected Builder(U command) {
        super(command);
      }
    }
  }

  /**
   * Abstract boolean query.
   */
  public static abstract class BooleanQuery<V> implements Query<V>, Writable {

    @Override
    public Consistency consistency() {
      return Consistency.LINEARIZABLE_STRICT;
    }

    @Override
    public void writeObject(Buffer buffer, Serializer serializer) {

    }

    @Override
    public void readObject(Buffer buffer, Serializer serializer) {

    }

    /**
     * Base boolean query builder.
     */
    public static abstract class Builder<T extends Builder<T, U>, U extends BooleanQuery<?>> extends Query.Builder<T, U> {
      protected Builder(U query) {
        super(query);
      }
    }
  }

  /**
   * Get query.
   */
  public static class Get extends BooleanQuery<Boolean> {

    /**
     * Returns a new get query builder.
     *
     * @return A new get query builder.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class);
    }

    /**
     * Get query builder.
     */
    public static class Builder extends BooleanQuery.Builder<Builder, Get> {
      public Builder(Get query) {
        super(query);
      }
    }
  }

  /**
   * Set command.
   */
  public static class Set extends BooleanCommand<Void> {

    /**
     * Returns a new set command builder.
     *
     * @return A new set command builder.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class);
    }

    private boolean value;

    /**
     * Returns the command value.
     *
     * @return The command value.
     */
    public boolean value() {
      return value;
    }

    @Override
    public void writeObject(Buffer buffer, Serializer serializer) {
      buffer.writeBoolean(value);
    }

    @Override
    public void readObject(Buffer buffer, Serializer serializer) {
      value = buffer.readBoolean();
    }

    /**
     * Put command builder.
     */
    public static class Builder extends BooleanCommand.Builder<Builder, Set> {
      public Builder(Set command) {
        super(command);
      }

      /**
       * Sets the command value.
       *
       * @param value The command value.
       * @return The command builder.
       */
      public Builder withValue(boolean value) {
        command.value = value;
        return this;
      }
    }
  }

  /**
   * Compare and set command.
   */
  public static class CompareAndSet extends BooleanCommand<Boolean> {

    /**
     * Returns a new compare and set command builder.
     *
     * @return A new compare and set command builder.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class);
    }

    private boolean expect;
    private boolean update;

    /**
     * Returns the expected value.
     *
     * @return The expected value.
     */
    public boolean expect() {
      return expect;
    }

    /**
     * Returns the updated value.
     *
     * @return The updated value.
     */
    public boolean update() {
      return update;
    }

    @Override
    public void writeObject(Buffer buffer, Serializer serializer) {
      buffer.writeBoolean(expect);
      buffer.writeBoolean(update);
    }

    @Override
    public void readObject(Buffer buffer, Serializer serializer) {
      expect = buffer.readBoolean();
      update = buffer.readBoolean();
    }

    /**
     * Compare and set command builder.
     */
    public static class Builder extends BooleanCommand.Builder<Builder, CompareAndSet> {
      public Builder(CompareAndSet command) {
        super(command);
      }

      /**
       * Sets the expected value.
       *
       * @param expect The expected value.
       * @return The command builder.
       */
      public Builder withExpect(boolean expect) {
        command.expect = expect;
        return this;
      }

      /**
       * Sets the updated value.
       *
       * @param update The updated value.
       * @return The command builder.
       */
      public Builder withUpdate(boolean update) {
        command.update = update;
        return this;
      }
    }
  }

  /**
   * Get and set command.
   */
  public static class GetAndSet extends BooleanCommand<Boolean> {

    /**
     * Returns a new get and set command builder.
     *
     * @return A new get and set command builder.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class);
    }

    private boolean value;

    /**
     * Returns the command value.
     *
     * @return The command value.
     */
    public boolean value() {
      return value;
    }

    @Override
    public void writeObject(Buffer buffer, Serializer serializer) {
      buffer.writeBoolean(value);
    }

    @Override
    public void readObject(Buffer buffer, Serializer serializer) {
      value = buffer.readBoolean();
    }

    /**
     * Put command builder.
     */
    public static class Builder extends BooleanCommand.Builder<Builder, GetAndSet> {
      public Builder(GetAndSet command) {
        super(command);
      }

      /**
       * Sets the command value.
       *
       * @param value The command value.
       * @return The command builder.
       */
      public Builder withValue(boolean value) {
        command.value = value;
        return this;
      }
    }
  }

  /**
   * Async boolean state machine.
   */
  public static class StateMachine extends net.kuujo.copycat.raft.StateMachine {
    private final AtomicBoolean value = new AtomicBoolean();
    private long version;

    /**
     * Handles a get commit.
     */
    @Apply(Get.class)
    protected boolean get(Commit<Get> commit) {
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
    protected boolean getAndSet(Commit<GetAndSet> commit) {
      boolean result = value.getAndSet(commit.operation().value());
      version = commit.index();
      return result;
    }

    /**
     * Filters all entries.
     */
    @Filter(Filter.All.class)
    protected boolean filterAll(Commit<? extends BooleanCommand<?>> commit, Compaction compaction) {
      return commit.index() >= version;
    }
  }

}
