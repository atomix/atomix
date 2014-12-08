/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.collections.internal;

import net.kuujo.copycat.Coordinator;
import net.kuujo.copycat.CopycatContext;
import net.kuujo.copycat.SubmitOptions;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.collections.AsyncCollection;
import net.kuujo.copycat.internal.AbstractResource;
import net.kuujo.copycat.internal.util.FluentList;

import java.util.concurrent.CompletableFuture;

/**
 * Abstract asynchronous collection implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AbstractAsyncCollection<T> extends AbstractResource implements AsyncCollection<T> {

  public AbstractAsyncCollection(String name, Coordinator coordinator, Cluster cluster, CopycatContext context) {
    super(name, coordinator, cluster, context);
  }

  @Override
  public CompletableFuture<Boolean> add(T value) {
    return context.submit(new FluentList().add("add").add(value), new SubmitOptions().withConsistent(true).withPersistent(false));
  }

  @Override
  public CompletableFuture<Boolean> remove(T value) {
    return context.submit(new FluentList().add("remove").add(value), new SubmitOptions().withConsistent(true).withPersistent(true));
  }

  @Override
  public CompletableFuture<Boolean> contains(Object value) {
    return context.submit(new FluentList().add("contains").add(value), new SubmitOptions().withConsistent(true).withPersistent(false));
  }

  @Override
  public CompletableFuture<Integer> size() {
    return context.submit(new FluentList().add("size"), new SubmitOptions().withConsistent(true).withPersistent(false));
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return context.submit(new FluentList().add("empty"), new SubmitOptions().withConsistent(true).withPersistent(false));
  }

  @Override
  public CompletableFuture<Void> clear() {
    return context.submit(new FluentList().add("clear"), new SubmitOptions().withConsistent(true).withPersistent(true));
  }

}
