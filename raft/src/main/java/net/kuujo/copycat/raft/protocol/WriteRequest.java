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
package net.kuujo.copycat.raft.protocol;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.util.ReferenceManager;

import java.util.Objects;

/**
 * Protocol write request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class WriteRequest extends AbstractRequest<WriteRequest> {
  private static final ThreadLocal<Builder> builder = new ThreadLocal<Builder>() {
    @Override
    protected Builder initialValue() {
      return new Builder();
    }
  };

  /**
   * Returns a new write request builder.
   *
   * @return A new write request builder.
   */
  public static Builder builder() {
    return builder.get().reset();
  }

  /**
   * Returns a write request builder for an existing request.
   *
   * @param request The request to build.
   * @return The write request builder.
   */
  public static Builder builder(WriteRequest request) {
    return builder.get().reset(request);
  }

  private Buffer key;
  private Buffer entry;

  public WriteRequest(ReferenceManager<WriteRequest> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.WRITE;
  }

  /**
   * Returns the write key.
   *
   * @return The write key.
   */
  public Buffer key() {
    return key;
  }

  /**
   * Returns the write entry.
   *
   * @return The write entry.
   */
  public Buffer entry() {
    return entry;
  }

  @Override
  public void readObject(Buffer buffer) {
    int keySize = buffer.readInt();
    if (keySize > -1) {
      key = buffer.slice(keySize);
      buffer.skip(keySize);
    }
    int entrySize = buffer.readInt();
    entry = buffer.slice(entrySize);
  }

  @Override
  public void writeObject(Buffer buffer) {
    if (key != null) {
      buffer.writeInt((int) key.limit()).write(key);
    } else {
      buffer.writeInt(-1);
    }
    buffer.writeInt((int) entry.limit()).write(entry);
  }

  @Override
  public void close() {
    if (key != null)
      key.release();
    entry.release();
    super.close();
  }

  @Override
  public int hashCode() {
    return Objects.hash(entry);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof WriteRequest) {
      WriteRequest request = (WriteRequest) object;
      return ((request.key == null && key == null) || (request.key != null && key != null && request.key.equals(key)))
        && request.entry.equals(entry);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[entry=%s]", getClass().getSimpleName(), entry.toString());
  }

  /**
   * Write request builder.
   */
  public static class Builder extends AbstractRequest.Builder<Builder, WriteRequest> {

    private Builder() {
      super(WriteRequest::new);
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

    /**
     * Sets the request entry.
     *
     * @param entry The request entry.
     * @return The request builder.
     */
    public Builder withEntry(Buffer entry) {
      if (entry == null)
        throw new NullPointerException("entry cannot be null");
      request.entry = entry;
      return this;
    }

    @Override
    public WriteRequest build() {
      super.build();
      if (request.entry == null)
        throw new NullPointerException("entry cannot be null");
      if (request.key != null)
        request.key.acquire();
      request.entry.acquire();
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
