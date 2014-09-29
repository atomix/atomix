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
package net.kuujo.copycat.log.impl;

import java.util.ArrayList;
import java.util.List;

import net.kuujo.copycat.log.Buffer;
import net.kuujo.copycat.log.EntryReader;
import net.kuujo.copycat.log.EntryType;
import net.kuujo.copycat.log.EntryWriter;

/**
 * State machine command entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@EntryType(id=1, reader=CommandEntry.Reader.class, writer=CommandEntry.Writer.class)
public class CommandEntry extends RaftEntry {
  private String command;
  private List<Object> args;

  private CommandEntry() {
    super();
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
  public String toString() {
    return String.format("CommandEntry[term=%d, command=%s, args=%s]", term, command, args);
  }

  public static class Reader implements EntryReader<CommandEntry> {
    @Override
    public CommandEntry readEntry(Buffer buffer) {
      CommandEntry entry = new CommandEntry();
      entry.term = buffer.getLong();
      int length = buffer.getInt();
      byte[] bytes = buffer.getBytes(length);
      entry.command = new String(bytes);
      entry.args = buffer.getCollection(new ArrayList<Object>(), Object.class);
      return entry;
    }
  }

  public static class Writer implements EntryWriter<CommandEntry> {
    @Override
    public void writeEntry(CommandEntry entry, Buffer buffer) {
      buffer.appendLong(entry.term);
      buffer.appendInt(entry.command.length());
      buffer.appendString(entry.command);
      buffer.appendCollection(entry.args);
    }
  }

}
