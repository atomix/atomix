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
package net.kuujo.copycat.protocol.raft.rpc;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.util.ReferenceManager;
import net.kuujo.copycat.protocol.raft.RaftError;

import java.util.Objects;

/**
 * Protocol delete response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DeleteResponse extends AbstractResponse<DeleteResponse> {
  private static final ThreadLocal<Builder> builder = new ThreadLocal<Builder>() {
    @Override
    protected Builder initialValue() {
      return new Builder();
    }
  };

  /**
   * Returns a new delete response builder.
   *
   * @return A new delete response builder.
   */
  public static Builder builder() {
    return builder.get().reset();
  }

  /**
   * Returns a delete response builder for an existing response.
   *
   * @param response The response to build.
   * @return The delete response builder.
   */
  public static Builder builder(DeleteResponse response) {
    return builder.get().reset(response);
  }

  private Buffer result;

  public DeleteResponse(ReferenceManager<DeleteResponse> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.DELETE;
  }

  /**
   * Returns the delete result.
   *
   * @return The delete result.
   */
  public Buffer result() {
    return result;
  }

  @Override
  public void readObject(Buffer buffer) {
    status = Response.Status.forId(buffer.readByte());
    if (status == Response.Status.OK) {
      error = null;
      result = buffer.slice();
    } else {
      error = RaftError.forId(buffer.readByte());
    }
  }

  @Override
  public void writeObject(Buffer buffer) {
    buffer.writeByte(status.id());
    if (status == Response.Status.OK) {
      buffer.write(result);
    } else {
      buffer.writeByte(error.id());
    }
  }

  @Override
  public void close() {
    result.release();
    super.close();
  }

  @Override
  public int hashCode() {
    return Objects.hash(status, result);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof DeleteResponse) {
      DeleteResponse response = (DeleteResponse) object;
      return response.status == status
        && ((response.result == null && result == null)
        || response.result != null && result != null && response.result.equals(result));
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, result=%s]", getClass().getSimpleName(), status, result);
  }

  /**
   * Delete response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, DeleteResponse> {

    private Builder() {
      super(DeleteResponse::new);
    }

    /**
     * Sets the delete response result.
     *
     * @param result The response result.
     * @return The response builder.
     */
    public Builder withResult(Buffer result) {
      response.result = result;
      return this;
    }

    @Override
    public DeleteResponse build() {
      super.build();
      response.result.acquire();
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
