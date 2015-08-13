/*
 * Copyright 2015 the original author or authors.
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

package net.kuujo.copycat.raft.server.state;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

/**
 * State machine clock.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class StateClock extends Clock {
  private final ZoneId zoneId = ZoneId.of("UTC");
  private Instant instant;

  /**
   * Sets the state machine clock instant.
   */
  void set(Instant instant) {
    this.instant = instant;
  }

  @Override
  public ZoneId getZone() {
    return zoneId;
  }

  @Override
  public Clock withZone(ZoneId zone) {
    throw new UnsupportedOperationException("cannot modify state machine time zone");
  }

  @Override
  public Instant instant() {
    return instant;
  }

}
