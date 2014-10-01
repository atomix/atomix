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
package net.kuujo.copycat.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.kuujo.copycat.StateMachine;

/**
 * Test state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TestStateMachine extends StateMachine {
  private final TestStateMachineEvents events;
  private final Map<String, Runnable> commandListeners = new HashMap<>();

  public TestStateMachine() {
    events = new TestStateMachineEvents(this);
  }

  /**
   * Returns test state machine event listeners.
   */
  public TestStateMachineEvents await() {
    return events;
  }

  void addCommandListener(String command, Runnable callback) {
    commandListeners.put(command, callback);
  }

  @Override
  public Object applyCommand(String command, List<Object> args) {
    Object result = super.applyCommand(command, args);
    Runnable listener = commandListeners.remove(command);
    if (listener != null) {
      listener.run();
    }
    return result;
  }

}
