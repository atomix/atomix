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
package net.kuujo.copycat.collections.state;

import net.kuujo.copycat.raft.Session;
import net.kuujo.copycat.raft.server.Apply;
import net.kuujo.copycat.raft.server.Commit;
import net.kuujo.copycat.raft.server.Filter;
import net.kuujo.copycat.raft.server.StateMachine;

import java.util.HashSet;
import java.util.Set;

/**
 * Topic state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TopicState extends StateMachine {
  private final Set<Session> sessions = new HashSet<>();

  @Override
  public void register(Session session) {
    sessions.add(session);
  }

  @Override
  public void expire(Session session) {
    sessions.remove(session);
  }

  @Override
  public void close(Session session) {
    sessions.remove(session);
  }

  /**
   * Handles a publish commit.
   */
  @Apply(TopicCommands.Publish.class)
  protected void applyPublish(Commit<TopicCommands.Publish> commit) {
    for (Session session : sessions) {
      session.publish(commit.operation().message());
    }
  }

  /**
   * Filters a publish commit.
   */
  @Filter(TopicCommands.Publish.class)
  protected boolean filterPublish(Commit<TopicCommands.Publish> commit) {
    return false;
  }

}
