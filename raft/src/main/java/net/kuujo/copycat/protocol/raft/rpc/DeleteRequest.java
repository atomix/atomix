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

import java.util.Objects;

/**
 * Protocol delete request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DeleteRequest extends AbstractRequest<DeleteRequest> {
  private static final ThreadLocal<Builder> builder = new ThreadLocal<Builder>() {
    @Override
    protected Builder initialValue() {
      return new Builder();
    }
  };

  /**
   * Returns a new delete request builder.
   *
   * @return A new delete request builder.
   */
  public static Builder builder() {
    return builder.get().reset();
  }

  /**
   * Returns a delete request builder for an existing request.
   *
   * @param request The request to build.
   * @return The delete request builder.
   */
  public static Builder builder(DeleteRequest request) {
    return builder.get().reset(request);
  }

  private Buffer key;

  public DeleteRequest(ReferenceManager<DeleteRequest> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.DELETE;
  }

  /**
   * Returns the delete key.
   *
   * @return The delete key.
   */
  public Buffer key() {
    return key;
  }

  @Override
  public void readObject(Buffer buffer) {
    key = buffer.slice();
  }

  @Override
  public void writeObject(Buffer buffer) {
    buffer.write(key);
  }

  @Override
  public void close() {
    key.release();
    super.close();
  }

  @Override
  public int hashCode() {
    return Objects.hash(key);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof DeleteRequest) {
      DeleteRequest request = (DeleteRequest) object;
      return request.key.equals(key);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[key=%s]", getClass().getSimpleName(), key.toString());
  }

  /**
   * Delete request builder.
   */
  public static class Builder extends AbstractRequest.Builder<Builder, DeleteRequest> {

    private Builder() {
      super(DeleteRequest::new);
    }

    /**
     * Sets the request key.
     *
     * @param key The request key.
     * @return The request builder.
     */
    public Builder withKey(Buffer key) {
      request.key = key;
      return this;
    }

    @Override
    public DeleteRequest build() {
      super.build();
      if (request.key == null)
        throw new NullPointerException("key cannot be null");
      request.key.acquire();
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
