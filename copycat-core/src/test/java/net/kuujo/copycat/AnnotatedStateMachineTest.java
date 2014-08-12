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

import java.util.HashMap;
import java.util.Map;

import net.kuujo.copycat.Command.Argument;

import org.junit.Assert;
import org.junit.Test;

/**
 * Annotated state machine test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AnnotatedStateMachineTest {

  @Test
  public void testGetCommandType() {
    AnnotatedStateMachine stateMachine = new TestGetCommandType();
    Command command = stateMachine.getCommand("foo");
    Assert.assertNotNull(command);
    Assert.assertEquals(Command.Type.READ, command.type());
  }

  private static class TestGetCommandType extends AnnotatedStateMachine {
    @Command(name="foo", type=Command.Type.READ)
    public String foo() {
      return "bar";
    }
  }

  @Test
  public void testApplyUnnamedCommand() {
    AnnotatedStateMachine stateMachine = new TestApplyUnnamedCommand();
    Assert.assertEquals("bar", stateMachine.applyCommand("foo", new HashMap<>()));
  }

  private static class TestApplyUnnamedCommand extends AnnotatedStateMachine {
    @Command
    public String foo() {
      return "bar";
    }
  }

  @Test
  public void testApplyNamedCommand() {
    AnnotatedStateMachine stateMachine = new TestApplyNamedCommand();
    Assert.assertEquals("bar", stateMachine.applyCommand("foo", new HashMap<>()));
  }

  private static class TestApplyNamedCommand extends AnnotatedStateMachine {
    @Command(name="foo")
    public String notFoo() {
      return "bar";
    }
  }

  @Test
  public void testApplyOptionalArgCommand() {
    AnnotatedStateMachine stateMachine = new TestApplyOptionalArgCommand();
    Map<String, Object> args1 = new HashMap<>();
    args1.put("arg1", "bar");
    Assert.assertEquals("bar", stateMachine.applyCommand("foo", args1));
    Assert.assertNull(stateMachine.applyCommand("foo", new HashMap<>()));
  }

  private static class TestApplyOptionalArgCommand extends AnnotatedStateMachine {
    @Command(name="foo")
    public String foo(@Argument(value="arg1", required=false) String returnValue) {
      return returnValue;
    }
  }

  @Test
  public void testApplyRequiredArgCommand() {
    AnnotatedStateMachine stateMachine = new TestApplyRequiredArgCommand();
    Map<String, Object> args1 = new HashMap<>();
    args1.put("arg1", "bar");
    Assert.assertEquals("bar", stateMachine.applyCommand("foo", args1));
    try {
      stateMachine.applyCommand("foo", new HashMap<>());
      Assert.fail("No exception thrown");
    } catch (CopyCatException e) {
    }
  }

  private static class TestApplyRequiredArgCommand extends AnnotatedStateMachine {
    @Command(name="foo")
    public String foo(@Argument(value="arg1", required=true) String returnValue) {
      return returnValue;
    }
  }

  @Test
  public void testApplyManyArgsCommand() {
    AnnotatedStateMachine stateMachine = new TestApplyManyArgsCommand();
    Map<String, Object> args = new HashMap<>();
    args.put("arg1", "foo");
    args.put("arg2", "bar");
    Assert.assertEquals("foobar", stateMachine.applyCommand("foo", args));
  }

  private static class TestApplyManyArgsCommand extends AnnotatedStateMachine {
    @Command(name="foo")
    public String foo(@Argument("arg1") String arg1, @Argument("arg2") String arg2) {
      return arg1 + arg2;
    }
  }

  @Test
  public void testTakeSnapshotWithField() {
    AnnotatedStateMachine stateMachine = new TakeSnapshotWithFieldStateMachine();
    Map<String, Object> snapshot = stateMachine.takeSnapshot();
    Assert.assertEquals("bar", snapshot.get("foo"));
  }

  private static class TakeSnapshotWithFieldStateMachine extends AnnotatedStateMachine {
    @Stateful
    private String foo = "bar";
  }

  @Test
  public void testInstallSnapshotWithField() {
    InstallSnapshotWithFieldStateMachine stateMachine = new InstallSnapshotWithFieldStateMachine();
    Map<String, Object> snapshot = new HashMap<>();
    snapshot.put("foo", "bar");
    stateMachine.installSnapshot(snapshot);
    Assert.assertEquals("bar", stateMachine.foo);
  }

  private static class InstallSnapshotWithFieldStateMachine extends AnnotatedStateMachine {
    @Stateful
    private String foo;
  }

}
