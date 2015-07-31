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
package net.kuujo.copycat.raft.server.log;

import net.kuujo.copycat.raft.Command;
import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.BufferOutput;
import net.kuujo.copycat.io.serializer.SerializeWith;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.log.Entry;
import net.kuujo.copycat.util.ReferenceManager;

/**
 * Command entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=306)
public class CommandEntry extends OperationEntry<CommandEntry> {
  private long request;
  private long response;
  private Command command;

  public CommandEntry(ReferenceManager<Entry<?>> referenceManager) {
    super(referenceManager);
  }

  /**
   * Returns the command request number.
   *
   * @return The command request number.
   */
  public long getRequest() {
    return request;
  }

  /**
   * Sets the command request number.
   *
   * @param request The command request number.
   * @return The command entry.
   */
  public CommandEntry setRequest(long request) {
    this.request = request;
    return this;
  }

  /**
   * Returns the command response number.
   *
   * @return The command response number.
   */
  public long getResponse() {
    return response;
  }

  /**
   * Sets the command response number.
   *
   * @param response The command response number.
   * @return The command entry.
   */
  public CommandEntry setResponse(long response) {
    this.response = response;
    return this;
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
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeLong(request).writeLong(response);
    serializer.writeObject(command, buffer);
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    request = buffer.readLong();
    response = buffer.readLong();
    command = serializer.readObject(buffer);
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, term=%d, session=%d, request=%d, response=%d, timestamp=%d, command=%s]", getClass().getSimpleName(), getIndex(), getTerm(), getSession(), request, response, getTimestamp(), command);
  }

}
