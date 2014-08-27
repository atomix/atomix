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
 * Events interface.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Events {

  /**
   * Start event constant.
   */
  public static Class<? extends Event> START = StartEvent.class;

  /**
   * Stop event constant.
   */
  public static Class<? extends Event> STOP = StopEvent.class;

  /**
   * Vote cast event constant.
   */
  public static Class<? extends Event> VOTE_CAST = VoteCastEvent.class;

  /**
   * Leader elected event constant.
   */
  public static Class<? extends Event> LEADER_ELECT = LeaderElectEvent.class;

  /**
   * State change event constant.
   */
  public static Class<? extends Event> STATE_CHANGE = StateChangeEvent.class;

  /**
   * Cluster membership change event constant.
   */
  public static Class<? extends Event> MEMBERSHIP_CHANGE = MembershipChangeEvent.class;

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
   * Returns a leader elected event context.
   *
   * @return The leader elected event context.
   */
  EventContext<LeaderElectEvent> leaderElect();

  /**
   * Returns a state change event context.
   *
   * @return The state change event context.
   */
  EventContext<StateChangeEvent> stateChange();

  /**
   * Returns a membership change event context.
   *
   * @return The membership change event context.
   */
  EventContext<MembershipChangeEvent> membershipChange();

}
