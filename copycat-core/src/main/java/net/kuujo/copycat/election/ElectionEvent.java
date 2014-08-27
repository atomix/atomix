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
package net.kuujo.copycat.election;

import net.kuujo.copycat.event.Event;

/**
 * Election event.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ElectionEvent implements Event {
  private final long term;
  private final String leader;

  public ElectionEvent(long term, String leader) {
    this.term = term;
    this.leader = leader;
  }

  /**
   * Returns the election term.
   *
   * @return The election term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns the election leader (if any).
   *
   * @return The election leader.
   */
  public String leader() {
    return leader;
  }

}
