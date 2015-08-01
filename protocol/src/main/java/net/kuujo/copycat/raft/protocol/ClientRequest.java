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

import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.BufferOutput;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.util.BuilderPool;
import net.kuujo.copycat.util.ReferenceFactory;
import net.kuujo.copycat.util.ReferenceManager;

/**
 * Client request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class ClientRequest<T extends ClientRequest<T>> extends AbstractRequest<T> {
  protected long version;

  public ClientRequest(ReferenceManager<T> referenceManager) {
    super(referenceManager);
  }

  /**
   * Returns the response version.
   *
   * @return The response version.
   */
  public long version() {
    return version;
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    buffer.writeLong(version);
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    version = buffer.readLong();
  }

  /**
   * Client request builder.
   */
  public static abstract class Builder<T extends Builder<T, U>, U extends ClientRequest<U>> extends AbstractRequest.Builder<T, U> {
    protected Builder(BuilderPool<T, U> pool, ReferenceFactory<U> factory) {
      super(pool, factory);
    }

    @Override
    protected void reset() {
      super.reset();
      request.version = 0;
    }

    /**
     * Sets the request version.
     *
     * @param version The request version.
     * @return The request builder.
     */
    @SuppressWarnings("unchecked")
    public T withVersion(long version) {
      request.version = version;
      return (T) this;
    }
  }

}
