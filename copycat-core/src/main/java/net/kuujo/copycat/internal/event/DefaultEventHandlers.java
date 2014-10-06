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
package net.kuujo.copycat.internal.event;

import net.kuujo.copycat.event.*;
import net.kuujo.copycat.event.EventHandlers;

/**
 * Default event handlers registry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultEventHandlers implements EventHandlers {
  private final DefaultEventHandlerRegistry<StartEvent> start = new DefaultEventHandlerRegistry<>();
  private final DefaultEventHandlerRegistry<StopEvent> stop = new DefaultEventHandlerRegistry<>();
  private final DefaultEventHandlerRegistry<VoteCastEvent> voteCast = new DefaultEventHandlerRegistry<>();
  private final DefaultEventHandlerRegistry<LeaderElectEvent> leaderElect = new DefaultEventHandlerRegistry<>();
  private final DefaultEventHandlerRegistry<MembershipChangeEvent> membershipChange = new DefaultEventHandlerRegistry<>();
  private final DefaultEventHandlerRegistry<StateChangeEvent> stateChange = new DefaultEventHandlerRegistry<>();

  @Override
  @SuppressWarnings("unchecked")
  public <E extends Event> DefaultEventHandlerRegistry<E> event(Class<E> event) {
    if (event == Events.START) {
      return (DefaultEventHandlerRegistry<E>) start;
    } else if (event == Events.STOP) {
      return (DefaultEventHandlerRegistry<E>) stop;
    } else if (event == Events.VOTE_CAST) {
      return (DefaultEventHandlerRegistry<E>) voteCast;
    } else if (event == Events.LEADER_ELECT) {
      return (DefaultEventHandlerRegistry<E>) leaderElect;
    } else if (event == Events.MEMBERSHIP_CHANGE) {
      return (DefaultEventHandlerRegistry<E>) membershipChange;
    } else if (event == Events.STATE_CHANGE) {
      return (DefaultEventHandlerRegistry<E>) stateChange;
    } else {
      throw new IllegalArgumentException("Unsupported event type");
    }
  }

  @Override
  public DefaultEventHandlerRegistry<StartEvent> start() {
    return start;
  }

  @Override
  public DefaultEventHandlerRegistry<StopEvent> stop() {
    return stop;
  }

  @Override
  public DefaultEventHandlerRegistry<VoteCastEvent> voteCast() {
    return voteCast;
  }

  @Override
  public DefaultEventHandlerRegistry<LeaderElectEvent> leaderElect() {
    return leaderElect;
  }

  @Override
  public DefaultEventHandlerRegistry<MembershipChangeEvent> membershipChange() {
    return membershipChange;
  }

  @Override
  public DefaultEventHandlerRegistry<StateChangeEvent> stateChange() {
    return stateChange;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
