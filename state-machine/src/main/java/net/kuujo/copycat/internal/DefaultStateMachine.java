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

import net.kuujo.copycat.*;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.internal.util.FluentList;

import java.util.concurrent.CompletableFuture;

/**
 * Default state machine implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultStateMachine extends AbstractResource implements StateMachine {
  private final StateModel model;
  private State currentState;
  private final StateContext stateContext = new StateContext() {
    @Override
    public State state() {
      return currentState;
    }
    @Override
    public StateContext transition(State state) {
      currentState = state;
      return this;
    }
  };

  public DefaultStateMachine(String name, Coordinator coordinator, Cluster cluster, CopycatContext context, StateModel model) {
    super(name, coordinator, cluster, context);
    this.model = model;
  }

  /**
   * Handles a state machine command.
   */
  private Object handle(Object entry) {
    if (entry instanceof FluentList) {
      FluentList list = (FluentList) entry;
      String command = list.get(0);
      Object arg = list.get(1);
      currentState.execute(command, arg, stateContext);
    }
    return null;
  }

  @Override
  public <T, U> CompletableFuture<U> submit(String command, T arg) {
    return context.submit(new FluentList().add(command).add(arg));
  }

}
