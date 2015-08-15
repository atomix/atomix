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
package net.kuujo.copycat.raft.protocol.request;

import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.BufferOutput;
import net.kuujo.copycat.io.serializer.SerializeWith;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.raft.protocol.Command;
import net.kuujo.copycat.util.BuilderPool;
import net.kuujo.copycat.util.ReferenceManager;

import java.util.Objects;

/**
 * Protocol command request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=258)
public class CommandRequest extends SessionRequest<CommandRequest> {

  /**
   * The unique identifier for the command request type.
   */
  public static final byte TYPE = 0x01;

  private static final BuilderPool<Builder, CommandRequest> POOL = new BuilderPool<>(Builder::new);

  /**
   * Returns a new submit request builder.
   *
   * @return A new submit request builder.
   */
  public static Builder builder() {
    return POOL.acquire();
  }

  /**
   * Returns a submit request builder for an existing request.
   *
   * @param request The request to build.
   * @return The submit request builder.
   */
  public static Builder builder(CommandRequest request) {
    return POOL.acquire(request);
  }

  private long commandSequence;
  private Command command;

  public CommandRequest(ReferenceManager<CommandRequest> referenceManager) {
    super(referenceManager);
  }

  @Override
  public byte type() {
    return TYPE;
  }

  /**
   * Returns the request sequence number.
   *
   * @return The request sequence number.
   */
  public long commandSequence() {
    return commandSequence;
  }

  /**
   * Returns the command.
   *
   * @return The command.
   */
  public Command command() {
    return command;
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    commandSequence = buffer.readLong();
    command = serializer.readObject(buffer);
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeLong(commandSequence);
    serializer.writeObject(command, buffer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session, commandSequence, command);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof CommandRequest) {
      CommandRequest request = (CommandRequest) object;
      return request.session == session
        && request.commandSequence == commandSequence
        && request.command.equals(command);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d, commandSequence=%d, command=%s]", getClass().getSimpleName(), session, commandSequence, command);
  }

  /**
   * Write request builder.
   */
  public static class Builder extends SessionRequest.Builder<Builder, CommandRequest> {

    protected Builder(BuilderPool<Builder, CommandRequest> pool) {
      super(pool, CommandRequest::new);
    }

    @Override
    protected void reset() {
      super.reset();
      request.commandSequence = 0;
      request.command = null;
    }

    /**
     * Sets the command sequence number.
     *
     * @param commandSequence The command sequence number.
     * @return The request builder.
     */
    public Builder withCommandSequence(long commandSequence) {
      if (commandSequence <= 0)
        throw new IllegalArgumentException("commandSequence cannot be less than 1");
      request.commandSequence = commandSequence;
      return this;
    }

    /**
     * Sets the request command.
     *
     * @param command The request command.
     * @return The request builder.
     */
    public Builder withCommand(Command command) {
      if (command == null)
        throw new NullPointerException("command cannot be null");
      request.command = command;
      return this;
    }

    @Override
    public CommandRequest build() {
      super.build();
      if (request.session <= 0)
        throw new IllegalArgumentException("session cannot be less than 1");
      if (request.commandSequence <= 0)
        throw new IllegalArgumentException("commandSequence cannot be less than 1");
      if (request.command == null)
        throw new NullPointerException("command cannot be null");
      return request;
    }

    @Override
    public int hashCode() {
      return Objects.hash(request);
    }

    @Override
    public boolean equals(Object object) {
      return object instanceof Builder && ((Builder) object).request.equals(request);
    }

    @Override
    public String toString() {
      return String.format("%s[request=%s]", getClass().getCanonicalName(), request);
    }

  }

}
