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
package net.kuujo.copycat.raft.protocol.response;

import net.kuujo.copycat.raft.protocol.error.RaftError;
import net.kuujo.copycat.util.*;

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

  /**
   * @throws NullPointerException if {@code referenceManager} is null
   */
  protected AbstractResponse(ReferenceManager<T> referenceManager) {
    this.referenceManager = Assert.notNull(referenceManager, "referenceManager");
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
  @SuppressWarnings("unchecked")
  public void release() {
    int refs = references.decrementAndGet();
    if (refs == 0) {
      referenceManager.release((T) this);
    } else if (refs < 0) {
      references.set(0);
      throw new IllegalStateException("cannot dereference non-referenced object");
    }
  }

  @Override
  public int references() {
    return references.get();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void close() {
    if (references.get() == 0)
      throw new IllegalStateException("cannot close non-referenced object");
    references.set(0);
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

    /**
     * @throws NullPointerException if {@code pool} or {@code factory} are null
     */
    protected Builder(BuilderPool<T, U> pool, ReferenceFactory<U> factory) {
      super(pool);
      this.pool = new ReferencePool<>(Assert.notNull(factory, "factory"));
    }

    @Override
    protected void reset() {
      response = pool.acquire();
      response.status = null;
      response.error = null;
    }

    @Override
    protected void reset(U response) {
      this.response = Assert.notNull(response, "response");
    }

    @Override
    @SuppressWarnings("unchecked")
    public T withStatus(Status status) {
      response.status = Assert.notNull(status, "status");
      return (T) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T withError(RaftError error) {
      response.error = Assert.notNull(error, "error");
      return (T) this;
    }

    /**
     * @throws IllegalStateException if status is null
     */
    @Override
    public U build() {
      Assert.stateNot(response.status == null, "status cannot be null");
      close();
      return response;
    }
  }

}
