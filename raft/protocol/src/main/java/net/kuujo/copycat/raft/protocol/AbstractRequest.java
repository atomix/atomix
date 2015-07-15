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

import net.kuujo.alleycat.util.ReferenceFactory;
import net.kuujo.alleycat.util.ReferenceManager;
import net.kuujo.alleycat.util.ReferencePool;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract request implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class AbstractRequest<T extends Request<T>> implements Request<T> {
  private final AtomicInteger references = new AtomicInteger();
  private ReferenceManager<T> referenceManager;

  protected AbstractRequest(ReferenceManager<T> referenceManager) {
    this.referenceManager = referenceManager;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T acquire() {
    references.incrementAndGet();
    return (T) this;
  }

  @Override
  public void release() {
    if (references.decrementAndGet() == 0)
      close();
  }

  @Override
  public int references() {
    return references.get();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void close() {
    referenceManager.release((T) this);
  }

  /**
   * Abstract request builder.
   *
   * @param <T> The builder type.
   * @param <U> The request type.
   */
  protected static abstract class Builder<T extends Builder<T, U>, U extends AbstractRequest<U>> implements Request.Builder<T, U> {
    protected final ReferencePool<U> pool;
    protected U request;

    protected Builder(ReferenceFactory<U> factory) {
      this.pool = new ReferencePool<>(factory);
    }

    /**
     * Resets the builder, acquiring a new request from the internal reference pool.
     */
    @SuppressWarnings("unchecked")
    T reset() {
      request = pool.acquire();
      return (T) this;
    }

    /**
     * Resets the builder with the given request.
     */
    @SuppressWarnings("unchecked")
    T reset(U request) {
      this.request = request;
      return (T) this;
    }

    @Override
    public U build() {
      return request;
    }
  }

}
