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
package io.atomix.protocols.raft.server.storage.entry;

import io.atomix.util.buffer.BufferInput;
import io.atomix.util.buffer.BufferOutput;

/**
 * Stores a state machine command.
 * <p>
 * The {@code CommandEntry} is used to store an individual state machine command from an individual
 * client along with information relevant to sequencing the command in the server state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CommandEntry extends OperationEntry<CommandEntry> {

  public CommandEntry(long timestamp, long session, long sequence, byte[] bytes) {
    super(timestamp, session, sequence, bytes);
  }

  @Override
  public Type<CommandEntry> type() {
    return Type.COMMAND;
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d, sequence=%d, timestamp=%d, command=byte[%d]]", getClass().getSimpleName(), session(), sequence(), timestamp(), bytes.length);
  }

  /**
   * Command entry serializer.
   */
  public static class Serializer implements OperationEntry.Serializer<CommandEntry> {
    @Override
    public void writeObject(BufferOutput output, CommandEntry entry) {
      output.writeLong(entry.timestamp);
      output.writeLong(entry.session);
      output.writeLong(entry.sequence);
      output.writeInt(entry.bytes.length);
      output.write(entry.bytes);
    }

    @Override
    public CommandEntry readObject(BufferInput input, Class<CommandEntry> type) {
      return new CommandEntry(input.readLong(), input.readLong(), input.readLong(), input.readBytes(input.readInt()));
    }
  }
}