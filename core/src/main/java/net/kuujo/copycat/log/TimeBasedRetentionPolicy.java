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
package net.kuujo.copycat.log;

import net.kuujo.copycat.spi.RetentionPolicy;

import java.util.concurrent.TimeUnit;

/**
 * Time based log compaction strategy.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TimeBasedRetentionPolicy implements RetentionPolicy {
  private long time;

  public TimeBasedRetentionPolicy() {
  }

  public TimeBasedRetentionPolicy(long time, TimeUnit unit) {
    this.time = unit.toMillis(time);
  }

  /**
   * Sets the retention time in milliseconds.
   *
   * @param time The retention time in milliseconds.
   */
  public void setTime(long time) {
    this.time = time;
  }

  /**
   * Sets the retention time.
   *
   * @param time The retention time.
   * @param unit The retention time unit.
   */
  public void setTime(long time, TimeUnit unit) {
    this.time = unit.toMillis(time);
  }

  /**
   * Returns the retention time in milliseconds.
   *
   * @return The retention time in milliseconds.
   */
  public long getTime() {
    return time;
  }

  /**
   * Sets the retention time in milliseconds, returning the retention policy for method chaining.
   *
   * @param time The retention time in milliseconds.
   * @return The retention policy.
   */
  public TimeBasedRetentionPolicy withTime(long time) {
    this.time = time;
    return this;
  }

  /**
   * Sets the retention time, returning the retention policy for method chaining.
   *
   * @param time The retention time.
   * @param unit The retention time unit.
   * @return The retention policy.
   */
  public TimeBasedRetentionPolicy withTime(long time, TimeUnit unit) {
    this.time = unit.toMillis(time);
    return this;
  }

  @Override
  public boolean retain(LogSegment segment) {
    return System.currentTimeMillis() < segment.timestamp() + time;
  }

}
