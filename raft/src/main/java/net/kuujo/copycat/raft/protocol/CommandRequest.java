/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.raft.protocol;

import net.kuujo.copycat.util.internal.Assert;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Protocol command request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CommandRequest extends AbstractRequest {

  /**
   * Returns a new command request builder.
   *
   * @return A new command request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns a command request builder for an existing request.
   *
   * @param request The request to build.
   * @return The command request builder.
   */
  public static Builder builder(CommandRequest request) {
    return new Builder(request);
  }

  private ByteBuffer entry;

  /**
   * Returns the command entry.
   *
   * @return The command entry.
   */
  public ByteBuffer entry() {
    return entry;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, entry);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof CommandRequest) {
      CommandRequest request = (CommandRequest) object;
      return request.id.equals(id)
        && request.entry.equals(entry);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[entry=%s]", getClass().getSimpleName(), entry.toString());
  }

  /**
   * Command request builder.
   */
  public static class Builder extends AbstractRequest.Builder<Builder, CommandRequest> {
    private Builder() {
      this(new CommandRequest());
    }

    private Builder(CommandRequest request) {
      super(request);
    }

    /**
     * Sets the request entry.
     *
     * @param entry The request entry.
     * @return The request builder.
     */
    public Builder withEntry(ByteBuffer entry) {
      request.entry = Assert.notNull(entry, "entry");
      return this;
    }

    @Override
    public CommandRequest build() {
      super.build();
      Assert.notNull(request.entry, "entry");
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
