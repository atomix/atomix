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
package net.kuujo.copycat.event;

/**
 * Vote cast event.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VoteCastEvent implements Event {
  private final long term;
  private final String candidate;

  public VoteCastEvent(long term, String candidate) {
    this.term = term;
    this.candidate = candidate;
  }

  /**
   * Returns the term in which the vote was cast.
   *
   * @return The term in which the vote was cast.
   */
  public long term() {
    return term;
  }

  /**
   * Returns the candidate for which the vote was cast.
   *
   * @return The candidate for which the vote was cast.
   */
  public String candidate() {
    return candidate;
  }

}
