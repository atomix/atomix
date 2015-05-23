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
import net.kuujo.copycat.raft.RaftError;

import java.util.Objects;

/**
 * Protocol poll response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class PollResponse extends AbstractResponse<PollResponse> {
  private static final ThreadLocal<Builder> builder = new ThreadLocal<Builder>() {
    @Override
    protected Builder initialValue() {
      return new Builder();
    }
  };

  /**
   * Returns a new poll response builder.
   *
   * @return A new poll response builder.
   */
  public static Builder builder() {
    return builder.get().reset();
  }

  /**
   * Returns a poll response builder for an existing response.
   *
   * @param response The response to build.
   * @return The poll response builder.
   */
  public static Builder builder(PollResponse response) {
    return builder.get().reset(response);
  }

  private long term;
  private boolean accepted;

  public PollResponse(ReferenceManager<PollResponse> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.POLL;
  }

  /**
   * Returns the responding node's current term.
   *
   * @return The responding node's current term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns a boolean indicating whether the poll was accepted.
   *
   * @return Indicates whether the poll was accepted.
   */
  public boolean accepted() {
    return accepted;
  }

  @Override
  public void readObject(Buffer buffer, Serializer serializer) {
    status = Response.Status.forId(buffer.readByte());
    if (status == Response.Status.OK) {
      error = null;
      term = buffer.readLong();
      accepted = buffer.readBoolean();
    } else {
      error = RaftError.forId(buffer.readByte());
    }
  }

  @Override
  public void writeObject(Buffer buffer, Serializer serializer) {
    buffer.writeByte(status.id());
    if (status == Response.Status.OK) {
      buffer.writeLong(term).writeBoolean(accepted);
    } else {
      buffer.writeByte(error.id());
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(status, term, accepted);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof PollResponse) {
      PollResponse response = (PollResponse) object;
      return response.status == status
        && response.term == term
        && response.accepted == accepted;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[term=%d, accepted=%b]", getClass().getSimpleName(), term, accepted);
  }

  /**
   * Poll response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, PollResponse> {

    private Builder() {
      super(PollResponse::new);
    }

    /**
     * Sets the response term.
     *
     * @param term The response term.
     * @return The poll response builder.
     */
    public Builder withTerm(long term) {
      if (term < 0)
        throw new IllegalArgumentException("term must be positive");
      response.term = term;
      return this;
    }

    /**
     * Sets whether the poll was granted.
     *
     * @param accepted Whether the poll was granted.
     * @return The poll response builder.
     */
    public Builder withAccepted(boolean accepted) {
      response.accepted = accepted;
      return this;
    }

    @Override
    public PollResponse build() {
      super.build();
      if (response.term < 0)
        throw new IllegalArgumentException("term must be positive");
      return response;
    }

    @Override
    public int hashCode() {
      return Objects.hash(response);
    }

    @Override
    public boolean equals(Object object) {
      return object instanceof Builder && ((Builder) object).response.equals(response);
    }

    @Override
    public String toString() {
      return String.format("%s[response=%s]", getClass().getCanonicalName(), response);
    }

  }

}
