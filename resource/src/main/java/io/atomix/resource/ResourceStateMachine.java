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
 * limitations under the License
 */
package io.atomix.resource;

import io.atomix.copycat.client.Command;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.StateMachineExecutor;

import java.util.function.Consumer;

/**
 * Base resource state machine.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class ResourceStateMachine extends StateMachine {

  @Override
  public final void init(StateMachineExecutor executor) {
    executor.register(DeleteCommand.class, (Consumer<Commit<DeleteCommand>>) c -> {
      try {
        delete();
      } finally {
        c.close();
      }
    });
    super.init(new ResourceStateMachineExecutor(executor));
  }

  /**
   * Deletes state machine state.
   */
  public void delete() {
  }

  /**
   * Resource delete command.
   */
  public static class DeleteCommand implements Command<Void> {
  }

}
