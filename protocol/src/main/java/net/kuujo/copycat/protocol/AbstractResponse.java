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
package net.kuujo.copycat.protocol;

import net.kuujo.copycat.BuilderPool;
import net.kuujo.copycat.RaftError;
import net.kuujo.copycat.util.ReferenceFactory;
import net.kuujo.copycat.util.ReferenceManager;
import net.kuujo.copycat.util.ReferencePool;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract response implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class AbstractResponse<T extends Response<T>> implements Response<T> {
  private final AtomicInteger references = new AtomicInteger();
  private final ReferenceManager<T> referenceManager;
  protected Status status = Status.OK;
  protected RaftError error;

  protected AbstractResponse(ReferenceManager<T> referenceManager) {
    this.referenceManager = referenceManager;
  }

  @Override
  public Status status() {
    return status;
  }

  @Override
  public RaftError error() {
    return error;
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

  @Override
  public String toString() {
    return String.format("%s[status=%s]", getClass().getCanonicalName(), status);
  }

  /**
   * Abstract response builder.
   *
   * @param <T> The builder type.
   * @param <U> The response type.
   */
  protected static abstract class Builder<T extends Builder<T, U>, U extends AbstractResponse<U>> extends Response.Builder<T, U> {
    protected final ReferencePool<U> pool;
    protected U response;

    protected Builder(BuilderPool<T, U> pool, ReferenceFactory<U> factory) {
      super(pool);
      this.pool = new ReferencePool<>(factory);
    }

    @Override
    protected void reset() {
      response = pool.acquire();
      response.status = null;
      response.error = null;
    }

    @Override
    protected void reset(U response) {
      this.response = response;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T withStatus(Status status) {
      if (status == null)
        throw new NullPointerException("status cannot be null");
      response.status = status;
      return (T) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T withError(RaftError error) {
      response.error = error;
      return (T) this;
    }

    @Override
    public U build() {
      if (response.status == null)
        throw new NullPointerException("status cannot be null");
      close();
      return response;
    }
  }

}
