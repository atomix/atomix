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
import java.util.function.Function;

/**
 * Protocol command request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class CommandRequest<REQUEST extends CommandRequest<REQUEST>> extends AbstractRequest<REQUEST> {
  protected Buffer key;
  protected Buffer entry;

  public CommandRequest(ReferenceManager<REQUEST> referenceManager) {
    super(referenceManager);
  }

  /**
   * Returns the command key.
   *
   * @return The command key.
   */
  public Buffer key() {
    return key;
  }

  /**
   * Returns the command entry.
   *
   * @return The command entry.
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
    if (entry != null)
      entry.release();
    super.close();
  }

  @Override
  public int hashCode() {
    return Objects.hash(entry);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof CommandRequest) {
      CommandRequest request = (CommandRequest) object;
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
  public static abstract class Builder<BUILDER extends Builder<BUILDER, REQUEST>, REQUEST extends CommandRequest<REQUEST>> extends AbstractRequest.Builder<BUILDER, REQUEST> {

    protected Builder(Function<ReferenceManager<REQUEST>, REQUEST> factory) {
      super(factory);
    }

    /**
     * Sets the request key.
     *
     * @param key The request key.
     * @return The request builder.
     */
    @SuppressWarnings("unchecked")
    public BUILDER withKey(Buffer key) {
      request.key = key;
      return (BUILDER) this;
    }

    /**
     * Sets the request entry.
     *
     * @param entry The request entry.
     * @return The request builder.
     */
    @SuppressWarnings("unchecked")
    public BUILDER withEntry(Buffer entry) {
      if (entry == null)
        throw new NullPointerException("entry cannot be null");
      request.entry = entry;
      return (BUILDER) this;
    }

    @Override
    public REQUEST build() {
      super.build();
      if (request.key != null)
        request.key.acquire();
      if (request.entry != null)
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
