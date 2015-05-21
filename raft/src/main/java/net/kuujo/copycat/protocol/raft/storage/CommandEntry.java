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
package net.kuujo.copycat.protocol.raft.storage;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.HeapBuffer;
import net.kuujo.copycat.io.util.ReferenceManager;

/**
 * Command entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CommandEntry extends SequencedEntry<CommandEntry> {
  private Buffer command = HeapBuffer.allocate(1024, 1024 * 1024);

  public CommandEntry(ReferenceManager<RaftEntry<?>> referenceManager) {
    super(referenceManager);
  }

  /**
   * Returns the command.
   *
   * @return The command.
   */
  public Buffer getCommand() {
    return command.flip();
  }

  /**
   * Sets the command.
   *
   * @param command The command.
   * @return The command entry.
   */
  public CommandEntry setCommand(Buffer command) {
    this.command.rewind().write(command);
    return this;
  }

  @Override
  public int size() {
    return super.size() + (int) command.position();
  }

  @Override
  public void writeObject(Buffer buffer) {
    super.writeObject(buffer);
    buffer.write(command.flip());
  }

  @Override
  public void readObject(Buffer buffer) {
    super.readObject(buffer);
    buffer.read(command.rewind());
  }

}
