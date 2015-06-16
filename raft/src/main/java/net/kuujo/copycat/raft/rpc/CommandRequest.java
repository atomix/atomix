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
package net.kuujo.copycat.raft.rpc;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.util.ReferenceManager;
import net.kuujo.copycat.raft.Command;

import java.util.Objects;

/**
 * Protocol command request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CommandRequest extends SessionRequest<CommandRequest> {
  private static final ThreadLocal<Builder> builder = new ThreadLocal<Builder>() {
    @Override
    protected Builder initialValue() {
      return new Builder();
    }
  };

  /**
   * Returns a new submit request builder.
   *
   * @return A new submit request builder.
   */
  public static Builder builder() {
    return builder.get().reset();
  }

  /**
   * Returns a submit request builder for an existing request.
   *
   * @param request The request to build.
   * @return The submit request builder.
   */
  public static Builder builder(CommandRequest request) {
    return builder.get().reset(request);
  }

  private long request;
  private long response;
  private Command command;

  public CommandRequest(ReferenceManager<CommandRequest> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.COMMAND;
  }

  /**
   * Returns the command request ID.
   *
   * @return The command request ID.
   */
  public long request() {
    return request;
  }

  /**
   * Returns the command response ID.
   *
   * @return The command response ID.
   */
  public long response() {
    return response;
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
  public void readObject(Buffer buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    request = buffer.readLong();
    response = buffer.readLong();
    command = serializer.readObject(buffer);
  }

  @Override
  public void writeObject(Buffer buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeLong(request).writeLong(response);
    serializer.writeObject(command, buffer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(command);
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof CommandRequest && ((CommandRequest) object).command.equals(command);
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d, command=%s]", getClass().getSimpleName(), session, command);
  }

  /**
   * Write request builder.
   */
  public static class Builder extends SessionRequest.Builder<Builder, CommandRequest> {

    protected Builder() {
      super(CommandRequest::new);
    }

    @Override
    Builder reset() {
      super.reset();
      request.request = 0;
      request.response = 0;
      request.command = null;
      return this;
    }

    /**
     * Sets the request ID.
     *
     * @param request The request ID.
     * @return The request builder.
     */
    public Builder withRequest(long request) {
      if (request <= 0)
        throw new IllegalArgumentException("request must be positive");
      this.request.request = request;
      return this;
    }

    /**
     * Sets the response ID.
     *
     * @param response The response ID.
     * @return The request builder.
     */
    public Builder withResponse(long response) {
      if (response < 0)
        throw new IllegalArgumentException("response must be positive");
      request.response = response;
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
        throw new IllegalArgumentException("session must be positive");
      if (request.request <= 0)
        throw new IllegalArgumentException("request must be positive");
      if (request.response < 0)
        throw new IllegalArgumentException("response must be positive");
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
