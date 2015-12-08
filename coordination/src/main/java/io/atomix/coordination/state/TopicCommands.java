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
import io.atomix.copycat.client.Command;

/**
 * Topic commands.
 * <p>
 * This class reserves serializable type IDs {@code 125} through {@code 127}
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class TopicCommands {

  private TopicCommands() {
  }

  /**
   * Abstract topic command.
   */
  public static abstract class TopicCommand<V> implements Command<V>, CatalystSerializable {
    @Override
    public CompactionMode compaction() {
      return CompactionMode.QUORUM_CLEAN;
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
  @SerializeWith(id=125)
  public static class Listen extends TopicCommand<Void> {
    @Override
    public CompactionMode compaction() {
      return CompactionMode.QUORUM_CLEAN;
    }

    @Override
    public ConsistencyLevel consistency() {
      return ConsistencyLevel.LINEARIZABLE;
    }
  }

  /**
   * Unlisten command.
   */
  @SerializeWith(id=126)
  public static class Unlisten extends TopicCommand<Void> {
    @Override
    public ConsistencyLevel consistency() {
      return ConsistencyLevel.LINEARIZABLE;
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.FULL_SEQUENTIAL_CLEAN;
    }
  }

  /**
   * Publish command.
   */
  @SerializeWith(id=127)
  public static class Publish<T> extends TopicCommand<Void> {
    private T message;

    public Publish() {
    }

    public Publish(T message) {
      this.message = message;
    }

    /**
     * Returns the publish message.
     *
     * @return The publish message.
     */
    public T message() {
      return message;
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.QUORUM_COMMIT;
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      serializer.writeObject(message, buffer);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      message = serializer.readObject(buffer);
    }
  }

}
