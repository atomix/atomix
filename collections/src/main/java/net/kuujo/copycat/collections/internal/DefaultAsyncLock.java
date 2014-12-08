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
import net.kuujo.copycat.collections.AsyncLock;
import net.kuujo.copycat.internal.AbstractResource;
import net.kuujo.copycat.internal.util.FluentList;

import java.util.concurrent.CompletableFuture;

/**
 * Default asynchronous lock implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultAsyncLock extends AbstractResource implements AsyncLock {

  public DefaultAsyncLock(String name, Coordinator coordinator, Cluster cluster, CopycatContext context) {
    super(name, coordinator, cluster, context);
  }

  @Override
  public CompletableFuture<Void> lock() {
    return context.submit(new FluentList().add("lock"), new SubmitOptions().withConsistent(true).withPersistent(true));
  }

  @Override
  public CompletableFuture<Void> unlock() {
    return context.submit(new FluentList().add("unlock"), new SubmitOptions().withConsistent(true).withPersistent(true));
  }

}
