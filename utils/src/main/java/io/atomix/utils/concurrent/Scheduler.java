/*
 * Copyright 2017-present Open Networking Laboratory
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

import java.time.Duration;

/**
 * Scheduler.
 */
public interface Scheduler {

  /**
   * Schedules a runnable on the context.
   *
   * @param callback The callback to schedule.
   * @param delay    The delay at which to schedule the runnable.
   */
  Scheduled schedule(Duration delay, Runnable callback);

  /**
   * Schedules a runnable at a fixed rate on the context.
   *
   * @param callback The callback to schedule.
   */
  Scheduled schedule(Duration initialDelay, Duration interval, Runnable callback);

}
