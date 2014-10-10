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
package net.kuujo.copycat.service;

import java.util.concurrent.CompletableFuture;

import net.kuujo.copycat.AsyncCopycat;
import net.kuujo.copycat.spi.service.AsyncService;

/**
 * Base service implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class BaseAsyncService implements AsyncService {
  protected AsyncCopycat copycat;

  @Override
  public void init(AsyncCopycat copycat) {
    this.copycat = copycat;
  }

  /**
   * Handles a command submission.
   */
  protected <T> CompletableFuture<T> submit(String command, Object... args) {
    if (copycat == null) {
      CompletableFuture<T> future = new CompletableFuture<>();
      future.completeExceptionally(new ServiceException("No submit handlers registered"));
      return future;
    }
    return copycat.submit(command, args);
  }

}
