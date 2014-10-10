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
package net.kuujo.copycat.internal;

import net.kuujo.copycat.AsyncCopycat;
import net.kuujo.copycat.AsyncCopycatContext;
import net.kuujo.copycat.spi.service.AsyncService;

import java.util.concurrent.CompletableFuture;

/**
 * Default asynchronous Copycat implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultAsyncCopycat extends AbstractCopycat<AsyncCopycatContext> implements AsyncCopycat {
  private final AsyncService service;

  public DefaultAsyncCopycat(AsyncService service, AsyncCopycatContext context) {
    super(context);
    this.service = service;
  }

  @Override
  public CompletableFuture<Void> start() {
    return context.start().thenRun(service::start);
  }

  @Override
  public CompletableFuture<Void> stop() {
    return service.stop().thenRun(context::stop);
  }

  @Override
  public String toString() {
    return String.format("%s[service=%s, context=%s]", getClass().getSimpleName(), service, context);
  }

}
