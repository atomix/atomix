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

import net.kuujo.copycat.util.Assert;
import net.kuujo.copycat.util.BuilderPool;
import net.kuujo.copycat.util.ReferenceFactory;
import net.kuujo.copycat.util.ReferenceManager;
import net.kuujo.copycat.util.ReferencePool;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract request implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class AbstractRequest<T extends Request<T>> implements Request<T> {
  private final AtomicInteger references = new AtomicInteger();
  private final ReferenceManager<T> referenceManager;

  /**
   * @throws NullPointerException if {@code referenceManager} is null
   */
  protected AbstractRequest(ReferenceManager<T> referenceManager) {
    this.referenceManager = Assert.notNull(referenceManager, "referenceManager");
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
  protected static abstract class Builder<T extends Builder<T, U>, U extends AbstractRequest<U>> extends Request.Builder<T, U> {
    protected final ReferencePool<U> pool;
    protected U request;

    /**
     * @throws NullPointerException if {@code pool} or {@code factory} are null
     */
    protected Builder(BuilderPool<T, U> pool, ReferenceFactory<U> factory) {
      super(pool);
      this.pool = new ReferencePool<>(factory);
    }

    @Override
    protected void reset() {
      request = pool.acquire();
    }

    @Override
    protected void reset(U request) {
      this.request = Assert.notNull(request, "request");
    }

    @Override
    public U build() {
      close();
      return request;
    }
  }

}
