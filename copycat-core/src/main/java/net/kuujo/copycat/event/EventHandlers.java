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
 * Event handlers registry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface EventHandlers {

  /**
   * Returns an event handler registry.
   *
   * @param event The event type.
   * @return The event handler registry.
   */
  <E extends Event> EventHandlerRegistry<E> event(Class<E> event);

  /**
   * Returns a start event handler registry.
   *
   * @return The start event handler registry.
   */
  EventHandlerRegistry<StartEvent> start();

  /**
   * Returns a stop event handler registry.
   *
   * @return The stop event handler registry.
   */
  EventHandlerRegistry<StopEvent> stop();

  /**
   * Returns a vote cast event handler registry.
   *
   * @return The vote cast event handler registry.
   */
  EventHandlerRegistry<VoteCastEvent> voteCast();

  /**
   * Returns a leader elect event handler registry.
   *
   * @return The leader elect event handler registry.
   */
  EventHandlerRegistry<LeaderElectEvent> leaderElect();

  /**
   * Returns a membership change event handler registry.
   *
   * @return The membership change event handler registry.
   */
  EventHandlerRegistry<MembershipChangeEvent> membershipChange();

  /**
   * Returns a state change event handler registry.
   *
   * @return The state change event handler registry.
   */
  EventHandlerRegistry<StateChangeEvent> stateChange();

}
