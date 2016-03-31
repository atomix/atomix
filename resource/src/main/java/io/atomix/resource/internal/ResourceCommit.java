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
 * limitations under the License
 */
package io.atomix.resource.internal;

import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.Operation;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.session.ServerSession;

import java.time.Instant;

/**
 * Wrapper for resource commits.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class ResourceCommit<T extends Operation> implements Commit<T> {
  private final Commit<? extends ResourceOperation> parent;

  public ResourceCommit(Commit<? extends ResourceOperation> parent) {
    this.parent = Assert.notNull(parent, "parent");
  }

  @Override
  public long index() {
    return parent.index();
  }

  @Override
  public ServerSession session() {
    return parent.session();
  }

  @Override
  public Instant time() {
    return parent.time();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Class<T> type() {
    return (Class<T>) parent.operation().getClass();
  }

  @Override
  @SuppressWarnings("unchecked")
  public T operation() {
    return (T) parent.operation().operation();
  }

  @Override
  public Commit<T> acquire() {
    parent.acquire();
    return this;
  }

  @Override
  public boolean release() {
    return parent.release();
  }

  @Override
  public int references() {
    return parent.references();
  }

  @Override
  public void close() {
    parent.close();
  }

  @Override
  public String toString() {
    return parent.toString();
  }

}
