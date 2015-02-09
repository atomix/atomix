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
package net.kuujo.copycat.cluster;

import net.kuujo.copycat.Event;

import java.util.Objects;
import java.util.UUID;

/**
 * Election event.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ElectionEvent implements Event<ElectionEvent.Type> {

  /**
   * Election event type.
   */
  public static enum Type {
    COMPLETE
  }

  private final String id = UUID.randomUUID().toString();
  private final Type type;
  private final long term;
  private final Member winner;

  public ElectionEvent(Type type, long term, Member winner) {
    this.type = type;
    this.term = term;
    this.winner = winner;
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public Type type() {
    return type;
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
   * Returns the winner of the election.
   *
   * @return The election winner.
   */
  public Member winner() {
    return winner;
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof ElectionEvent && ((ElectionEvent) object).id.equals(id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, type, term, winner);
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s, type=%s, term=%d, winner=%s]", getClass().getSimpleName(), id, type, term, winner);
  }

}
