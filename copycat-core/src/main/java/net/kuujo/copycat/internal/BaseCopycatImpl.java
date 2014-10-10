/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.internal;

import net.kuujo.copycat.BaseCopycat;
import net.kuujo.copycat.CopycatConfig;
import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.event.Event;
import net.kuujo.copycat.event.EventContext;
import net.kuujo.copycat.event.EventHandlerRegistry;
import net.kuujo.copycat.event.EventHandlers;
import net.kuujo.copycat.event.Events;
import net.kuujo.copycat.internal.event.DefaultEvents;
import net.kuujo.copycat.internal.state.StateContext;
import net.kuujo.copycat.internal.util.Assert;

/**
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class BaseCopycatImpl implements BaseCopycat {
  protected final StateContext state;
  protected final Cluster<?> cluster;
  protected final CopycatConfig config;
  protected final Events events;

  protected BaseCopycatImpl(StateContext state, Cluster<?> cluster, CopycatConfig config) {
    this.state = state;
    this.cluster = cluster;
    this.config = config;
    this.events = new DefaultEvents(state.events());
  }
  
  @Override
  public CopycatConfig config() {
    return config;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <M extends Member> Cluster<M> cluster() {
    return (Cluster<M>) cluster;
  }

  @Override
  public Events on() {
    return events;
  }

  @Override
  public <T extends Event> EventContext<T> on(Class<T> event) {
    return events.event(Assert.isNotNull(event, "Event cannot be null"));
  }

  @Override
  public EventHandlers events() {
    return state.events();
  }

  @Override
  public <T extends Event> EventHandlerRegistry<T> event(Class<T> event) {
    return state.events().event(Assert.isNotNull(event, "Event cannot be null"));
  }

  @Override
  public CopycatState state() {
    return state.state();
  }

  @Override
  public String leader() {
    return state.currentLeader();
  }

  @Override
  public boolean isLeader() {
    return state.isLeader();
  }
}
