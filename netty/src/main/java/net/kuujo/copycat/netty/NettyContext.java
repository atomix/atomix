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
package net.kuujo.copycat.netty;

import net.kuujo.copycat2.spi.Context;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Netty context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NettyContext implements Context {

  @Override
  public long schedule(Runnable task, long delay, TimeUnit unit) {
    return 0;
  }

  @Override
  public void cancel(long task) {

  }

  @Override
  public void execute(Runnable task) {

  }

  @Override
  public <T> CompletableFuture<T> execute(Callable<T> task) {
    return null;
  }

}
