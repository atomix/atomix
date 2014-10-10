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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import net.kuujo.copycat.Copycat;
import net.kuujo.copycat.CopycatConfig;
import net.kuujo.copycat.CopycatException;
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
public class SyncCopycatImpl extends BaseCopycatImpl implements Copycat {
  public SyncCopycatImpl(StateContext state, Cluster<?> cluster, CopycatConfig config) {
    super(state, cluster, config);
  }

  @Override
  public void start() {
    CountDownLatch latch = new CountDownLatch(1);
    state.start().thenRun(latch::countDown);
    try {
      latch.await(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new CopycatException(e);
    };
  }

  @Override
  public void stop() {
    CountDownLatch latch = new CountDownLatch(1);
    state.stop().thenRun(latch::countDown);
    try {
      latch.await(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new CopycatException(e);
    };
  }

  @Override
  @SuppressWarnings("unchecked")
  public <R> R submit(final String operation, final Object... args) {
    final CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<R> result = new AtomicReference<>();
    state.submit(Assert.isNotNull(operation, "operation cannot be null"), args).whenComplete(
        (r, error) -> {
          latch.countDown();
          result.set((R) r);
        });
    try {
      latch.await(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new CopycatException(e);
    }
    return result.get();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
