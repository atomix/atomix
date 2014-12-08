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
package net.kuujo.copycat.protocol;

import net.kuujo.copycat.spi.ExecutionContext;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.spi.VertxSPI;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Vert.x protocol context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VertxExecutionContext implements ExecutionContext {
  private final Vertx vertx;

  public VertxExecutionContext(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public long schedule(Runnable task, long delay, TimeUnit unit) {
    return vertx.setTimer(unit.toMillis(delay), (timerId) -> task.run());
  }

  @Override
  public void cancel(long task) {
    vertx.cancelTimer(task);
  }

  @Override
  public void execute(Runnable task) {
    vertx.runOnContext((v) -> task.run());
  }

  @Override
  public <T> CompletableFuture<T> submit(Callable<T> task) {
    CompletableFuture<T> future = new CompletableFuture<>();
    ((VertxSPI) vertx).executeBlocking(() -> {
      try {
        return task.call();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, result -> {
      if (result.failed()) {
        future.completeExceptionally(result.cause());
      } else {
        future.complete(result.result());
      }
    });
    return future;
  }

}
