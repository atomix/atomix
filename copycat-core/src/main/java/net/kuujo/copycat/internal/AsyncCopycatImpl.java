/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.internal;

import java.util.concurrent.CompletableFuture;

import net.kuujo.copycat.AsyncCopycat;
import net.kuujo.copycat.CopycatConfig;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.internal.state.StateContext;
import net.kuujo.copycat.internal.util.Assert;

/**
 * Primary copycat API.
 * <p>
 *
 * The <code>CopyCat</code> class provides a fluent API for combining the
 * {@link DefaultCopycatContext} with an {@link net.kuujo.copycat.spi.service.Service}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AsyncCopycatImpl extends AbstractCopycat implements AsyncCopycat {
  public AsyncCopycatImpl(StateContext state, Cluster<?> cluster, CopycatConfig config) {
    super(state, cluster, config);
  }

  @Override
  public CompletableFuture<Void> start() {
    return state.start();
  }

  @Override
  public CompletableFuture<Void> stop() {
    return state.stop();
  }

  @Override
  public <R> CompletableFuture<R> submit(final String operation, final Object... args) {
    return state.submit(Assert.isNotNull(operation, "operation cannot be null"), args);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
