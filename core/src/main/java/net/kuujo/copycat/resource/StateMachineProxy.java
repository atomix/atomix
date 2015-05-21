/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.resource;

/**
 * State machine proxy.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StateMachineProxy extends StateMachine {
  private final StateMachine stateMachine;
  private long version;
  private long timestamp;

  public StateMachineProxy(StateMachine stateMachine) {
    this.stateMachine = stateMachine;
  }

  /**
   * Returns the current version of the state machine state.
   */
  public long version() {
    return version;
  }

  /**
   * Returns the current timestamp of the state machine state.
   */
  public long timestamp() {
    return timestamp;
  }

  @Override
  public Object apply(Commit<?> commit) {
    Object result = stateMachine.apply(commit);
    version = commit.index();
    timestamp = commit.timestamp();
    return result;
  }

  @Override
  public boolean filter(Commit<?> commit) {
    return stateMachine.filter(commit);
  }

}
