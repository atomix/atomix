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
package net.kuujo.copycat.protocol;

import net.kuujo.copycat.Callback;

/**
 * Timer strategy.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface TimerStrategy {

  /**
   * Starts a new timer.
   *
   * @param delay The delay after which to trigger the callback.
   * @param callback The callback to trigger.
   * @return The unique timer ID.
   */
  long startTimer(long delay, Callback<Long> callback);

  /**
   * Cancels a timer.
   *
   * @param id The timer ID.
   */
  void cancelTimer(long id);

}
