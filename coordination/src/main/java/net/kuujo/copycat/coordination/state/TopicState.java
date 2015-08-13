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
package net.kuujo.copycat.coordination.state;

import net.kuujo.copycat.raft.Session;
import net.kuujo.copycat.raft.server.Commit;
import net.kuujo.copycat.raft.server.StateMachine;
import net.kuujo.copycat.raft.server.StateMachineExecutor;

/**
 * Topic state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TopicState extends StateMachine {

  @Override
  public void configure(StateMachineExecutor executor) {
    executor.register(TopicCommands.Publish.class, this::publish);
  }

  /**
   * Handles a publish commit.
   */
  protected void publish(Commit<TopicCommands.Publish> commit) {
    for (Session session : context().sessions()) {
      session.publish(commit.operation().message());
    }
    commit.clean();
  }

}
