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
package net.kuujo.copycat.protocol.impl;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import net.kuujo.copycat.protocol.TimerStrategy;

/**
 * Thread-based timer strategy.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ThreadTimerStrategy implements TimerStrategy {
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);

  @Override
  @SuppressWarnings("unchecked")
  public ScheduledFuture<Void> schedule(Runnable task, long delay, TimeUnit unit) {
    return (ScheduledFuture<Void>) scheduler.schedule(task, delay, unit);
  }

}
