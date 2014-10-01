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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Test state machine events.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TestStateMachineEvents {
  private final TestStateMachine stateMachine;

  public TestStateMachineEvents(TestStateMachine stateMachine) {
    this.stateMachine = stateMachine;
  }

  /**
   * Listens for a command to be applied to the state machine.
   *
   * @param command The command for which to listen.
   * @return The events object.
   */
  public TestStateMachineEvents commandApplied(String command) {
    final CountDownLatch latch = new CountDownLatch(1);
    stateMachine.addCommandListener(command, () -> latch.countDown());
    try {
      latch.await(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

}
