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
package io.atomix.coordination.state;

import io.atomix.copycat.client.session.Session;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.StateMachineExecutor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Topic state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TopicState extends StateMachine {
  private final Map<Long, Commit<TopicCommands.Listen>> listeners = new HashMap<>();

  @Override
  public void configure(StateMachineExecutor executor) {
    executor.register(TopicCommands.Listen.class, this::listen);
    executor.register(TopicCommands.Unlisten.class, this::unlisten);
    executor.register(TopicCommands.Publish.class, this::publish);
  }

  @Override
  public void close(Session session) {
    listeners.remove(session.id());
  }

  /**
   * Applies listen commits.
   */
  protected void listen(Commit<TopicCommands.Listen> commit) {
    if (!listeners.containsKey(commit.session().id())) {
      listeners.put(commit.session().id(), commit);
    } else {
      commit.clean();
    }
  }

  /**
   * Applies listen commits.
   */
  protected void unlisten(Commit<TopicCommands.Unlisten> commit) {
    try {
      Commit<TopicCommands.Listen> listener = listeners.remove(commit.session().id());
      if (listener != null) {
        listener.clean();
      }
    } finally {
      commit.clean();
    }
  }

  /**
   * Handles a publish commit.
   */
  protected void publish(Commit<TopicCommands.Publish> commit) {
    try {
      Iterator<Map.Entry<Long, Commit<TopicCommands.Listen>>> iterator = listeners.entrySet().iterator();
      while (iterator.hasNext()) {
        Commit<TopicCommands.Listen> listener = iterator.next().getValue();
        if (listener.session().isOpen()) {
          listener.session().publish("message", commit.operation().message());
        } else {
          iterator.remove();
          listener.clean();
        }
      }
    } finally {
      commit.clean();
    }
  }

}
