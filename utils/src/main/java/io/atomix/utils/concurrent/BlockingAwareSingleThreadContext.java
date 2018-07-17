/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.utils.concurrent;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Blocking aware single thread context.
 */
public class BlockingAwareSingleThreadContext extends SingleThreadContext {
  private final Executor threadPoolExecutor;

  public BlockingAwareSingleThreadContext(String nameFormat, Executor threadPoolExecutor) {
    this(namedThreads(nameFormat, LOGGER), threadPoolExecutor);
  }

  public BlockingAwareSingleThreadContext(ThreadFactory factory, Executor threadPoolExecutor) {
    super(factory);
    this.threadPoolExecutor = threadPoolExecutor;
  }

  @Override
  public void execute(Runnable command) {
    if (isBlocked()) {
      threadPoolExecutor.execute(command);
    } else {
      super.execute(command);
    }
  }
}
