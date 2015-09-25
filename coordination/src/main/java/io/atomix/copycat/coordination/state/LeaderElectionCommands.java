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
package io.atomix.copycat.coordination.state;

import io.atomix.catalogue.client.Command;
import io.atomix.catalogue.client.Operation;
import io.atomix.catalogue.client.Query;
import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.BuilderPool;

/**
 * Leader election commands.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LeaderElectionCommands {

  private LeaderElectionCommands() {
  }

  /**
   * Abstract election query.
   */
  public static abstract class ElectionQuery<V> implements Query<V>, CatalystSerializable {
    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
    }

    /**
     * Base reference command builder.
     */
    public static abstract class Builder<T extends Builder<T, U, V>, U extends ElectionQuery<V>, V> extends Query.Builder<T, U, V> {
      protected Builder(BuilderPool<T, U> pool) {
        super(pool);
      }
    }
  }

  /**
   * Abstract election command.
   */
  public static abstract class ElectionCommand<V> implements Command<V>, CatalystSerializable {
    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
    }

    /**
     * Base reference command builder.
     */
    public static abstract class Builder<T extends Builder<T, U, V>, U extends ElectionCommand<V>, V> extends Command.Builder<T, U, V> {
      protected Builder(BuilderPool<T, U> pool) {
        super(pool);
      }
    }
  }

  /**
   * Listen command.
   */
  @SerializeWith(id=510)
  public static class Listen extends ElectionCommand<Void> {

    /**
     * Returns a new listen command builder.
     *
     * @return A new listen command builder.
     */
    @SuppressWarnings("unchecked")
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Listen command builder.
     */
    public static class Builder extends ElectionCommand.Builder<Builder, Listen, Void> {
      public Builder(BuilderPool<Builder, Listen> pool) {
        super(pool);
      }

      @Override
      protected Listen create() {
        return new Listen();
      }
    }
  }

  /**
   * Unlisten command.
   */
  @SerializeWith(id=511)
  public static class Unlisten extends ElectionCommand<Void> {

    /**
     * Returns a new unlisten command builder.
     *
     * @return A new unlisten command builder.
     */
    @SuppressWarnings("unchecked")
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Unlisten command builder.
     */
    public static class Builder extends ElectionCommand.Builder<Builder, Unlisten, Void> {
      public Builder(BuilderPool<Builder, Unlisten> pool) {
        super(pool);
      }

      @Override
      protected Unlisten create() {
        return new Unlisten();
      }
    }
  }

  /**
   * Is leader query.
   */
  @SerializeWith(id=512)
  public static class IsLeader extends ElectionQuery<Boolean> {

    /**
     * Returns a new is leader query builder.
     *
     * @return A new is leader query builder.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    private long epoch;

    /**
     * Returns the epoch to check.
     *
     * @return The epoch to check.
     */
    public long epoch() {
      return epoch;
    }

    @Override
    public ConsistencyLevel consistency() {
      return ConsistencyLevel.LINEARIZABLE;
    }

    /**
     * Is leader query builder.
     */
    public static class Builder extends ElectionQuery.Builder<Builder, IsLeader, Boolean> {
      public Builder(BuilderPool<Builder, IsLeader> pool) {
        super(pool);
      }

      @Override
      protected IsLeader create() {
        return new IsLeader();
      }

      /**
       * Sets the epoch to check.
       *
       * @param epoch The epoch to check.
       * @return The query builder.
       */
      public Builder withEpoch(long epoch) {
        if (epoch <= 0)
          throw new IllegalArgumentException("epoch must be positive");
        query.epoch = epoch;
        return this;
      }
    }
  }

}
