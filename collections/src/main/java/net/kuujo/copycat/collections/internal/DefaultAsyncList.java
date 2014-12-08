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
import net.kuujo.copycat.collections.AsyncList;
import net.kuujo.copycat.internal.util.FluentList;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Default asynchronous list implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultAsyncList<T> extends AbstractAsyncCollection<T> implements AsyncList<T> {
  private final List<T> list = new ArrayList<>();

  public DefaultAsyncList(String name, Coordinator coordinator, Cluster cluster, CopycatContext state) {
    super(name, coordinator, cluster, state);
  }

  /**
   * Handles an entry.
   */
  private Object handle(Object entry) {
    if (entry instanceof FluentList) {
      FluentList list = (FluentList) entry;
      String action = list.get(0);
      switch (action) {
        case "get":
          return this.list.get(list.get(1));
        case "set":
          this.list.set(list.get(1), list.get(2));
          return null;
        case "remove":
          return this.list.remove(list.get(1));
      }
    }
    return null;
  }

  @Override
  public CompletableFuture<T> get(int index) {
    return context.submit(new FluentList().add("get").add(index), new SubmitOptions().withConsistent(true).withPersistent(false));
  }

  @Override
  public CompletableFuture<Void> set(int index, T value) {
    return context.submit(new FluentList().add("set").add(index).add(value), new SubmitOptions().withConsistent(true).withPersistent(true));
  }

  @Override
  public CompletableFuture<T> remove(int index) {
    return context.submit(new FluentList().add("remove").add(index), new SubmitOptions().withConsistent(true).withPersistent(true));
  }

}
