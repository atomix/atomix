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
package net.kuujo.copycat.raft.storage;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.util.ReferenceManager;
import net.kuujo.copycat.raft.Command;

/**
 * Command entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CommandEntry extends SequencedEntry<CommandEntry> {
  private Command command;

  public CommandEntry(ReferenceManager<RaftEntry<?>> referenceManager) {
    super(referenceManager);
  }

  /**
   * Returns the command.
   *
   * @return The command.
   */
  public Command getCommand() {
    return command;
  }

  /**
   * Sets the command.
   *
   * @param command The command.
   * @return The command entry.
   */
  public CommandEntry setCommand(Command command) {
    this.command = command;
    return this;
  }

  @Override
  public void writeObject(Buffer buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    serializer.writeObject(command, buffer);
  }

  @Override
  public void readObject(Buffer buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    command = serializer.readObject(buffer);
  }

}
