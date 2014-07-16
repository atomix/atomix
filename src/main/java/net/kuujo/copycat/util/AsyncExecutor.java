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
package net.kuujo.copycat.util;

import java.util.concurrent.Executor;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.impl.DefaultFutureResult;

/**
 * Asynchronous action executor.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AsyncExecutor {
  private final Executor executor;

  public AsyncExecutor(Executor executor) {
    this.executor = executor;
  }

  /**
   * Executes an asynchronous action.
   *
   * @param action The action to execute.
   * @param resultHandler An asynchronous handler to be called with the
   *        execution result.
   */
  public <T> void execute(final AsyncAction<T> action, final Handler<AsyncResult<T>> resultHandler) {
    executor.execute(new Runnable() {
      @Override
      public void run() {
        final DefaultFutureResult<T> result = new DefaultFutureResult<>();
        try {
          result.setResult(action.execute());
        } catch (Exception e) {
          result.setFailure(e);
        }
        if (resultHandler != null) {
          executor.execute(new Runnable() {
            @Override
            public void run() {
              result.setHandler(resultHandler);
            }
          });
        }
      }
    });
  }

}
