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
package net.kuujo.copycat;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.ArrayList;

import org.testng.annotations.Test;

/**
 * Annotated state machine test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class StateMachineTest {
  public void testGetCommandType() {
    StateMachine stateMachine = new TestGetCommandType();
    Command command = stateMachine.getCommand("foo");
    assertNotNull(command);
    assertEquals(Command.Type.READ, command.type());
  }

  private static class TestGetCommandType extends StateMachine {
    @Command(type = Command.Type.READ)
    public String foo() {
      return "bar";
    }
  }

  public void testApplyUnnamedCommand() {
    StateMachine stateMachine = new TestApplyUnnamedCommand();
    assertEquals("bar", stateMachine.applyCommand("foo", new ArrayList<>(0)));
  }

  private static class TestApplyUnnamedCommand extends StateMachine {
    @Command
    public String foo() {
      return "bar";
    }
  }

  public void testApplyNamedCommand() {
    StateMachine stateMachine = new TestApplyNamedCommand();
    assertEquals("bar", stateMachine.applyCommand("foo", new ArrayList<>(0)));
  }

  private static class TestApplyNamedCommand extends StateMachine {
    @Command(name = "foo")
    public String notFoo() {
      return "bar";
    }
  }

}
