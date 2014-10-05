/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.log.internal;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import net.kuujo.copycat.log.EntryType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * State machine command entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@EntryType(id=3, serializer=CommandEntry.Serializer.class)
public class CommandEntry extends CopycatEntry {
  private String command;
  private List<Object> args;

  private CommandEntry() {
    super();
  }

  public CommandEntry(long term, String command, Object... args) {
    this(term, command, Arrays.asList(args));
  }

  public CommandEntry(long term, String command, List<Object> args) {
    super(term);
    this.command = command;
    this.args = args;
  }

  /**
   * Returns the state machine command.
   * 
   * @return The state machine command.
   */
  public String command() {
    return command;
  }

  /**
   * Returns command arguments.
   *
   * @return The command arguments.
   */
  public List<Object> args() {
    return args;
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof CommandEntry) {
      CommandEntry entry = (CommandEntry) object;
      return term == entry.term && command.equals(entry.command) && args.equals(entry.args);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + (int)(term ^ (term >>> 32));
    hashCode = 37 * hashCode + command.hashCode();
    hashCode = 37 * hashCode + args.hashCode();
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("CommandEntry[term=%d, command=%s, args=%s]", term, command, args);
  }

  /**
   * Command entry serializer.
   *
   * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
   */
  public static class Serializer extends com.esotericsoftware.kryo.Serializer<CommandEntry> {
    @Override
    @SuppressWarnings("unchecked")
    public CommandEntry read(Kryo kryo, Input input, Class<CommandEntry> type) {
      CommandEntry entry = new CommandEntry();
      entry.term = input.readLong();
      int commandLength = input.readInt();
      byte[] commandBytes = new byte[commandLength];
      input.readBytes(commandBytes);
      entry.command = new String(commandBytes);
      entry.args = kryo.readObject(input, ArrayList.class);
      return entry;
    }
    @Override
    public void write(Kryo kryo, Output output, CommandEntry entry) {
      output.writeLong(entry.term);
      byte[] commandBytes = entry.command.getBytes();
      output.writeInt(commandBytes.length);
      output.writeBytes(commandBytes);
      kryo.writeObject(output, entry.args);
    }
  }

}
