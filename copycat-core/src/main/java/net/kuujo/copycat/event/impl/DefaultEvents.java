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
import net.kuujo.copycat.event.Events;
import net.kuujo.copycat.event.LeaderElectEvent;
import net.kuujo.copycat.event.MembershipChangeEvent;
import net.kuujo.copycat.event.StartEvent;
import net.kuujo.copycat.event.StateChangeEvent;
import net.kuujo.copycat.event.StopEvent;
import net.kuujo.copycat.event.VoteCastEvent;

/**
 * Default events API implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultEvents implements Events {
  private final DefaultEventContext<StartEvent> start = new DefaultEventContext<>();
  private final DefaultEventContext<StopEvent> stop = new DefaultEventContext<>();
  private final DefaultEventContext<VoteCastEvent> voteCast = new DefaultEventContext<>();
  private final DefaultEventContext<LeaderElectEvent> leaderElect = new DefaultEventContext<>();
  private final DefaultEventContext<StateChangeEvent> stateChange = new DefaultEventContext<>();
  private final DefaultEventContext<MembershipChangeEvent> membershipChange = new DefaultEventContext<>();

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Event> DefaultEventContext<T> event(Class<T> event) {
    if (event == StartEvent.class) {
      return (DefaultEventContext<T>) start();
    } else if (event == StopEvent.class) {
      return (DefaultEventContext<T>) stop();
    } else if (event == VoteCastEvent.class) {
      return (DefaultEventContext<T>) voteCast();
    } else if (event == LeaderElectEvent.class) {
      return (DefaultEventContext<T>) leaderElect();
    } else if (event == StateChangeEvent.class) {
      return (DefaultEventContext<T>) stateChange();
    } else if (event == MembershipChangeEvent.class) {
      return (DefaultEventContext<T>) membershipChange();
    } else {
      throw new UnsupportedOperationException("Unsupported event type");
    }
  }

  @Override
  public DefaultEventContext<StartEvent> start() {
    return start;
  }

  @Override
  public DefaultEventContext<StopEvent> stop() {
    return stop;
  }

  @Override
  public DefaultEventContext<VoteCastEvent> voteCast() {
    return voteCast;
  }

  @Override
  public DefaultEventContext<LeaderElectEvent> leaderElect() {
    return leaderElect;
  }

  @Override
  public DefaultEventContext<StateChangeEvent> stateChange() {
    return stateChange;
  }

  @Override
  public DefaultEventContext<MembershipChangeEvent> membershipChange() {
    return membershipChange;
  }

}
