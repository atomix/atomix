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
 * Events context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface EventsContext {

  /**
   * Returns an event context.
   *
   * @param event The event for which to return a context.
   * @return The event context.
   */
  <T extends Event> EventContext<T> event(Class<T> event);

  /**
   * Returns a start event context.
   *
   * @return The start event context.
   */
  EventContext<StartEvent> start();

  /**
   * Returns a stop event context.
   *
   * @return The stop event context.
   */
  EventContext<StopEvent> stop();

  /**
   * Returns a vote cast event context.
   *
   * @return The vote cast event context.
   */
  EventContext<VoteCastEvent> voteCast();

  /**
   * Returns a leader elect event context.
   *
   * @return The leader elect event context.
   */
  EventContext<LeaderElectEvent> leaderElect();

  /**
   * Returns a membership change event context.
   *
   * @return The membership change event context.
   */
  EventContext<MembershipChangeEvent> membershipChange();

  /**
   * Returns a state change event context.
   *
   * @return The state change event context.
   */
  EventContext<StateChangeEvent> stateChange();

}
