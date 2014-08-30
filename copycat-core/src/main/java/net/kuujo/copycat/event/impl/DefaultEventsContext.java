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
package net.kuujo.copycat.event.impl;

import net.kuujo.copycat.event.Event;
import net.kuujo.copycat.event.EventContext;
import net.kuujo.copycat.event.EventHandlersRegistry;
import net.kuujo.copycat.event.Events;
import net.kuujo.copycat.event.EventsContext;
import net.kuujo.copycat.event.LeaderElectEvent;
import net.kuujo.copycat.event.MembershipChangeEvent;
import net.kuujo.copycat.event.StartEvent;
import net.kuujo.copycat.event.StateChangeEvent;
import net.kuujo.copycat.event.StopEvent;
import net.kuujo.copycat.event.VoteCastEvent;

/**
 * Default events context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultEventsContext implements EventsContext {
  private final EventContext<StartEvent> start;
  private final EventContext<StopEvent> stop;
  private final EventContext<VoteCastEvent> voteCast;
  private final EventContext<LeaderElectEvent> leaderElect;
  private final EventContext<MembershipChangeEvent> membershipChange;
  private final EventContext<StateChangeEvent> stateChange;

  public DefaultEventsContext(EventHandlersRegistry registry) {
    start = new DefaultEventContext<>(registry.start());
    stop = new DefaultEventContext<>(registry.stop());
    voteCast = new DefaultEventContext<>(registry.voteCast());
    leaderElect = new DefaultEventContext<>(registry.leaderElect());
    membershipChange = new DefaultEventContext<>(registry.membershipChange());
    stateChange = new DefaultEventContext<>(registry.stateChange());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <E extends Event> EventContext<E> event(Class<E> event) {
    if (event == Events.START) {
      return (EventContext<E>) start;
    } else if (event == Events.STOP) {
      return (EventContext<E>) stop;
    } else if (event == Events.VOTE_CAST) {
      return (EventContext<E>) voteCast;
    } else if (event == Events.LEADER_ELECT) {
      return (EventContext<E>) leaderElect;
    } else if (event == Events.MEMBERSHIP_CHANGE) {
      return (EventContext<E>) membershipChange;
    } else if (event == Events.STATE_CHANGE) {
      return (EventContext<E>) stateChange;
    } else {
      throw new IllegalArgumentException("Unsupported event type");
    }
  }

  @Override
  public EventContext<StartEvent> start() {
    return start;
  }

  @Override
  public EventContext<StopEvent> stop() {
    return stop;
  }

  @Override
  public EventContext<VoteCastEvent> voteCast() {
    return voteCast;
  }

  @Override
  public EventContext<LeaderElectEvent> leaderElect() {
    return leaderElect;
  }

  @Override
  public EventContext<MembershipChangeEvent> membershipChange() {
    return membershipChange;
  }

  @Override
  public EventContext<StateChangeEvent> stateChange() {
    return stateChange;
  }

}
