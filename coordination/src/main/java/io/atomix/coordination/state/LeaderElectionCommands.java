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
package io.atomix.coordination.state;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Query;

/**
 * Leader election commands.
 * <p>
 * This class reserves serializable type IDs {@code 110} through {@code 114}
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class LeaderElectionCommands {

  private LeaderElectionCommands() {
  }

  /**
   * Abstract election query.
   */
  public static abstract class ElectionQuery<V> implements Query<V>, CatalystSerializable {

    @Override
    public ConsistencyLevel consistency() {
      return ConsistencyLevel.LINEARIZABLE;
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
    }
  }

  /**
   * Abstract election command.
   */
  public static abstract class ElectionCommand<V> implements Command<V>, CatalystSerializable {
    @Override
    public CompactionMode compaction() {
      return CompactionMode.QUORUM;
    }

    @Override
    public ConsistencyLevel consistency() {
      return ConsistencyLevel.LINEARIZABLE;
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
    }
  }

  /**
   * Listen command.
   */
  @SerializeWith(id=110)
  public static class Listen extends ElectionCommand<Void> {
    @Override
    public CompactionMode compaction() {
      return CompactionMode.QUORUM;
    }
  }

  /**
   * Unlisten command.
   */
  @SerializeWith(id=111)
  public static class Unlisten extends ElectionCommand<Void> {
    @Override
    public CompactionMode compaction() {
      return CompactionMode.SEQUENTIAL;
    }
  }

  /**
   * Is leader query.
   */
  @SerializeWith(id=112)
  public static class IsLeader extends ElectionQuery<Boolean> {
    private long epoch;

    public IsLeader() {
    }

    public IsLeader(long epoch) {
      this.epoch = Assert.argNot(epoch, epoch < 0, "epoch cannot be negative");
    }

    /**
     * Returns the epoch to check.
     *
     * @return The epoch to check.
     */
    public long epoch() {
      return epoch;
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      buffer.writeLong(epoch);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      epoch = buffer.readLong();
    }

    @Override
    public ConsistencyLevel consistency() {
      return ConsistencyLevel.LINEARIZABLE;
    }
  }

  /**
   * Resign command.
   */
  @SerializeWith(id=113)
  public static class Resign extends ElectionCommand<Void> {
    private long epoch;

    public Resign() {
    }

    public Resign(long epoch) {
      this.epoch = Assert.argNot(epoch, epoch < 0, "epoch cannot be negative");
    }

    /**
     * Returns the epoch for which to resign.
     *
     * @return The epoch for which to resign.
     */
    public long epoch() {
      return epoch;
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      buffer.writeLong(epoch);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      epoch = buffer.readLong();
    }

    @Override
    public ConsistencyLevel consistency() {
      return ConsistencyLevel.LINEARIZABLE;
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.SEQUENTIAL;
    }
  }

}
