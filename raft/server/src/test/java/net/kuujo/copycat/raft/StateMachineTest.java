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
package net.kuujo.copycat.raft;

import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * State machine test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class StateMachineTest {

  /**
   * Tests a state machine.
   */
  public void testStateMachine() {
    StateMachine stateMachine = new TestStateMachine();
    Assert.assertEquals(stateMachine.apply(new Commit<>(1, null, System.currentTimeMillis(), new TestCommand())), "write");
    Assert.assertEquals(stateMachine.apply(new Commit<>(1, null, System.currentTimeMillis(), new TestQuery())), "read");
  }

  /**
   * Test command.
   */
  public static class TestCommand implements Command<String> {
  }

  /**
   * Test query.
   */
  public static class TestQuery implements Query<String> {
    @Override
    public ConsistencyLevel consistency() {
      return ConsistencyLevel.LINEARIZABLE;
    }
  }

  /**
   * Test state machine.
   */
  public static class TestStateMachine extends StateMachine {
    @Apply(TestCommand.class)
    public String applyCommand(Commit<TestCommand> commit) {
      return "write";
    }

    @Filter(TestCommand.class)
    public boolean filterCommand(Commit<TestCommand> commit) {
      return true;
    }

    @Apply(TestQuery.class)
    public String applyQuery(Commit<TestQuery> commit) {
      return "read";
    }
  }

}
