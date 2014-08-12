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

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import net.kuujo.copycat.Callback;
import net.kuujo.copycat.protocol.TimerStrategy;

/**
 * Thread-based timer strategy.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ThreadTimerStrategy implements TimerStrategy {
  private final Timer timer = new Timer();
  private final Map<Long, TimerTask> tasks = new HashMap<>();
  private long id;

  @Override
  public long startTimer(long delay, final Callback<Long> callback) {
    final long id = ++this.id;
    TimerTask task = new TimerTask() {
      @Override
      public void run() {
        tasks.remove(id);
        callback.call(id);
      }
    };
    tasks.put(id, task);
    timer.schedule(task, delay);
    return id;
  }

  @Override
  public void cancelTimer(long id) {
    TimerTask task = tasks.remove(id);
    if (task != null) {
      task.cancel();
      timer.purge();
    }
  }

}
