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
package net.kuujo.copycat.test.unit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import net.kuujo.copycat.AnnotatedStateMachine;
import net.kuujo.copycat.CommandInfo;
import net.kuujo.copycat.Stateful;

import org.junit.Test;
import org.vertx.java.core.json.JsonObject;

/**
 * State machine tests.
 *
 * @author Jordan Halterman
 */
public class StateMachineTest {

  public static class TestStateMachine extends AnnotatedStateMachine {

    @CommandInfo(name="one")
    public String one() {
      return "one";
    }

    @CommandInfo(name="two")
    public Object two(@CommandInfo.Value JsonObject args) {
      return args.getString("foo");
    }

    @CommandInfo(name="three")
    public Boolean three(String arg0, Boolean arg1) {
      return arg1;
    }

    @CommandInfo(name="four", type=CommandInfo.Type.WRITE)
    public String four(@CommandInfo.Argument("foo") String foo) {
      return foo;
    }

    @CommandInfo(name="five", type=CommandInfo.Type.READ)
    public String five(@CommandInfo.Argument("foo") String foo, @CommandInfo.Argument("bar") Boolean bar) {
      return foo;
    }

    @CommandInfo(name="six", type=CommandInfo.Type.READ_WRITE)
    public Object six(@CommandInfo.Argument(value="foo", required=false) String foo) {
      return foo;
    }

    @CommandInfo(name="seven", type=CommandInfo.Type.READ_WRITE)
    public Object seven(@CommandInfo.Argument("foo") String foo, @CommandInfo.Argument("bar") Boolean bar, @CommandInfo.Argument(value="baz", required=false) Object baz) {
      return foo;
    }

  }

  @Test
  public void testNoArgCommand() {
    AnnotatedStateMachine stateMachine = new TestStateMachine();
    CommandInfo command = stateMachine.getCommandInfo("one");
    assertNotNull(command);
    assertEquals("one", command.name());
    assertEquals(CommandInfo.Type.READ_WRITE, command.type());
    assertEquals("one", stateMachine.applyCommand("one", new JsonObject()).getString("result"));
  }

  @Test
  public void testDefaultArgCommand() {
    AnnotatedStateMachine stateMachine = new TestStateMachine();
    CommandInfo command = stateMachine.getCommandInfo("two");
    assertNotNull(command);
    assertEquals("two", command.name());
    assertEquals(CommandInfo.Type.READ_WRITE, command.type());
    assertEquals("bar", stateMachine.applyCommand("two", new JsonObject().putString("foo", "bar")).getString("result"));
  }

  @Test
  public void testAutoNamedArgsCommand() {
    AnnotatedStateMachine stateMachine = new TestStateMachine();
    CommandInfo command = stateMachine.getCommandInfo("three");
    assertNotNull(command);
    assertEquals("three", command.name());
    assertEquals(CommandInfo.Type.READ_WRITE, command.type());
    assertEquals(true, stateMachine.applyCommand("three", new JsonObject().putString("arg0", "Hello world!").putBoolean("arg1", true)).getBoolean("result"));
  }

  @Test
  public void testAutoNamedArgsCommandFail() {
    AnnotatedStateMachine stateMachine = new TestStateMachine();
    CommandInfo command = stateMachine.getCommandInfo("three");
    assertNotNull(command);
    assertEquals("three", command.name());
    assertEquals(CommandInfo.Type.READ_WRITE, command.type());
    try {
      stateMachine.applyCommand("three", new JsonObject().putString("arg0", "Hello world!"));
      fail("Failed to prevent application.");
    }
    catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testNamedArgCommand() {
    AnnotatedStateMachine stateMachine = new TestStateMachine();
    CommandInfo command = stateMachine.getCommandInfo("four");
    assertNotNull(command);
    assertEquals("four", command.name());
    assertEquals(CommandInfo.Type.WRITE, command.type());
    assertEquals("Hello world!", stateMachine.applyCommand("four", new JsonObject().putString("foo", "Hello world!")).getString("result"));
  }

  @Test
  public void testNamedArgCommandFail() {
    AnnotatedStateMachine stateMachine = new TestStateMachine();
    CommandInfo command = stateMachine.getCommandInfo("four");
    assertNotNull(command);
    assertEquals("four", command.name());
    assertEquals(CommandInfo.Type.WRITE, command.type());
    try {
      stateMachine.applyCommand("four", new JsonObject());
      fail("Failed to prevent application.");
    }
    catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testNamedArgsCommand() {
    AnnotatedStateMachine stateMachine = new TestStateMachine();
    CommandInfo command = stateMachine.getCommandInfo("five");
    assertNotNull(command);
    assertEquals("five", command.name());
    assertEquals(CommandInfo.Type.READ, command.type());
    assertEquals("Hello world!", stateMachine.applyCommand("five", new JsonObject().putString("foo", "Hello world!").putBoolean("bar", true)).getString("result"));
  }

  @Test
  public void testNamedArgsCommandFail() {
    AnnotatedStateMachine stateMachine = new TestStateMachine();
    CommandInfo command = stateMachine.getCommandInfo("five");
    assertNotNull(command);
    assertEquals("five", command.name());
    assertEquals(CommandInfo.Type.READ, command.type());
    try {
      stateMachine.applyCommand("five", new JsonObject().putBoolean("bar", true));
      fail("Failed to prevent application.");
    }
    catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testOptionalArgCommand() {
    AnnotatedStateMachine stateMachine = new TestStateMachine();
    CommandInfo command = stateMachine.getCommandInfo("six");
    assertNotNull(command);
    assertEquals("six", command.name());
    assertEquals(CommandInfo.Type.READ_WRITE, command.type());
    assertEquals("Hello world!", stateMachine.applyCommand("six", new JsonObject().putString("foo", "Hello world!")).getString("result"));
    assertNull(stateMachine.applyCommand("six", new JsonObject().putString("something", "else")).getString("result"));
  }

  @Test
  public void testOptionalArgsCommand() {
    AnnotatedStateMachine stateMachine = new TestStateMachine();
    CommandInfo command = stateMachine.getCommandInfo("seven");
    assertNotNull(command);
    assertEquals("seven", command.name());
    assertEquals(CommandInfo.Type.READ_WRITE, command.type());
    assertEquals("Hello world!", stateMachine.applyCommand("seven", new JsonObject().putString("foo", "Hello world!").putBoolean("bar", true).putString("baz", "Something else")).getString("result"));
    assertEquals("Hello world!", stateMachine.applyCommand("seven", new JsonObject().putString("foo", "Hello world!").putBoolean("bar", true)));
  }

  public static class TestSnapshotStateMachine extends AnnotatedStateMachine {
    @Stateful
    private final Map<String, Object> data = new HashMap<>();

    @CommandInfo(name="write", type=CommandInfo.Type.WRITE)
    public void write(@CommandInfo.Argument("key") String key, @CommandInfo.Argument("value") Object value) {
      data.put(key, value);
    }
  }

  @Test
  public void testTakeSnapshotFromField() {
    TestSnapshotStateMachine stateMachine = new TestSnapshotStateMachine();
    stateMachine.applyCommand("write", new JsonObject().putString("key", "foo").putValue("value", "bar"));
    stateMachine.applyCommand("write", new JsonObject().putString("key", "bar").putValue("value", "baz"));
    stateMachine.applyCommand("write", new JsonObject().putString("key", "baz").putValue("value", "foo"));
    JsonObject snapshot = stateMachine.createSnapshot();
    assertTrue(snapshot instanceof JsonObject);
    assertEquals("bar", snapshot.getObject("data").getString("foo"));
    assertEquals("baz", snapshot.getObject("data").getString("bar"));
    assertEquals("foo", snapshot.getObject("data").getString("baz"));
  }

  @Test
  public void testInstallSnapshotToField() {
    TestSnapshotStateMachine stateMachine = new TestSnapshotStateMachine();
    stateMachine.installSnapshot(new JsonObject()
        .putObject("data", new JsonObject()
        .putString("foo", "bar")
        .putString("bar", "baz")
        .putString("baz", "foo")));
    JsonObject snapshot = stateMachine.createSnapshot();
    assertTrue(snapshot instanceof JsonObject);
    assertEquals("bar", snapshot.getObject("data").getString("foo"));
    assertEquals("baz", snapshot.getObject("data").getString("bar"));
    assertEquals("foo", snapshot.getObject("data").getString("baz"));
  }

  public static class TestSnapshotStateMachine2 extends AnnotatedStateMachine {
    private Map<String, Object> data = new HashMap<>();

    @CommandInfo(name="write", type=CommandInfo.Type.WRITE)
    public void write(@CommandInfo.Argument("key") String key, @CommandInfo.Argument("value") Object value) {
      data.put(key, value);
    }

    @Stateful("data")
    public JsonObject doTakeSnapshot() {
      return new JsonObject(data);
    }

    @Stateful("data")
    public void doInstallSnapshot(Map<String, Object> snapshot) {
      this.data = snapshot;
    }
  }

  @Test
  public void testTakeSnapshotFromMethod() {
    TestSnapshotStateMachine2 stateMachine = new TestSnapshotStateMachine2();
    stateMachine.applyCommand("write", new JsonObject().putString("key", "foo").putValue("value", "bar"));
    stateMachine.applyCommand("write", new JsonObject().putString("key", "bar").putValue("value", "baz"));
    stateMachine.applyCommand("write", new JsonObject().putString("key", "baz").putValue("value", "foo"));
    JsonObject snapshot = stateMachine.createSnapshot();
    assertEquals("bar", snapshot.getObject("data").getString("foo"));
    assertEquals("baz", snapshot.getObject("data").getString("bar"));
    assertEquals("foo", snapshot.getObject("data").getString("baz"));
  }

  @Test
  public void testInstallSnapshotToMethod() {
    TestSnapshotStateMachine2 stateMachine = new TestSnapshotStateMachine2();
    stateMachine.installSnapshot(new JsonObject()
        .putObject("data", new JsonObject()
            .putString("foo", "bar")
            .putString("bar", "baz")
            .putString("baz", "foo")));
    JsonObject snapshot = stateMachine.createSnapshot();
    assertTrue(snapshot instanceof JsonObject);
    assertEquals("bar", snapshot.getObject("data").getString("foo"));
    assertEquals("baz", snapshot.getObject("data").getString("bar"));
    assertEquals("foo", snapshot.getObject("data").getString("baz"));
  }

  public static class TestSnapshotStateMachine3 extends AnnotatedStateMachine {
    @Stateful
    private Map<String, Object> data = new HashMap<>();

    @CommandInfo(name="write", type=CommandInfo.Type.WRITE)
    public void write(@CommandInfo.Argument("key") String key, @CommandInfo.Argument("value") Object value) {
      data.put(key, value);
    }

    public JsonObject doTakeSnapshot() {
      return new JsonObject(data);
    }

    public void doInstallSnapshot(Map<String, Object> snapshot) {
      this.data = snapshot;
    }
  }

  @Test
  public void testTakeSnapshotFromDetectedMethod() {
    TestSnapshotStateMachine3 stateMachine = new TestSnapshotStateMachine3();
    stateMachine.applyCommand("write", new JsonObject().putString("key", "foo").putValue("value", "bar"));
    stateMachine.applyCommand("write", new JsonObject().putString("key", "bar").putValue("value", "baz"));
    stateMachine.applyCommand("write", new JsonObject().putString("key", "baz").putValue("value", "foo"));
    JsonObject snapshot = stateMachine.createSnapshot();
    assertEquals("bar", snapshot.getObject("data").getString("foo"));
    assertEquals("baz", snapshot.getObject("data").getString("bar"));
    assertEquals("foo", snapshot.getObject("data").getString("baz"));
  }

  @Test
  public void testInstallSnapshotToDetectedMethod() {
    TestSnapshotStateMachine3 stateMachine = new TestSnapshotStateMachine3();
    stateMachine.installSnapshot(new JsonObject()
        .putObject("data", new JsonObject().putString("foo", "bar")
            .putString("bar", "baz")
            .putString("baz", "foo")));
    JsonObject snapshot = stateMachine.createSnapshot();
    assertTrue(snapshot instanceof JsonObject);
    assertEquals("bar", snapshot.getObject("data").getString("foo"));
    assertEquals("baz", snapshot.getObject("data").getString("bar"));
    assertEquals("foo", snapshot.getObject("data").getString("baz"));
  }

  public static class TestSnapshotStateMachine4 extends AnnotatedStateMachine {
    @Stateful("foo")
    private Map<String, Object> data = new HashMap<>();

    @CommandInfo(name="write", type=CommandInfo.Type.WRITE)
    public void write(@CommandInfo.Argument("key") String key, @CommandInfo.Argument("value") Object value) {
      data.put(key, value);
    }

    public JsonObject doTakeSnapshot() {
      return new JsonObject(data);
    }

    public void doInstallSnapshot(Map<String, Object> snapshot) {
      this.data = snapshot;
    }
  }

  @Test
  public void testTakeSnapshotFromNamedField() {
    TestSnapshotStateMachine4 stateMachine = new TestSnapshotStateMachine4();
    stateMachine.applyCommand("write", new JsonObject().putString("key", "foo").putValue("value", "bar"));
    stateMachine.applyCommand("write", new JsonObject().putString("key", "bar").putValue("value", "baz"));
    stateMachine.applyCommand("write", new JsonObject().putString("key", "baz").putValue("value", "foo"));
    JsonObject snapshot = stateMachine.createSnapshot();
    assertEquals("bar", snapshot.getObject("foo").getString("foo"));
    assertEquals("baz", snapshot.getObject("foo").getString("bar"));
    assertEquals("foo", snapshot.getObject("foo").getString("baz"));
  }

  @Test
  public void testInstallSnapshotToNamedField() {
    TestSnapshotStateMachine4 stateMachine = new TestSnapshotStateMachine4();
    stateMachine.installSnapshot(new JsonObject()
        .putObject("foo", new JsonObject()
            .putString("foo", "bar")
            .putString("bar", "baz")
            .putString("baz", "foo")));
    JsonObject snapshot = stateMachine.createSnapshot();
    assertTrue(snapshot instanceof JsonObject);
    assertEquals("bar", snapshot.getObject("foo").getString("foo"));
    assertEquals("baz", snapshot.getObject("foo").getString("bar"));
    assertEquals("foo", snapshot.getObject("foo").getString("baz"));
  }

  public static class TestSnapshotStateMachine5 extends AnnotatedStateMachine {
    @Stateful("foo")
    private Map<String, Object> data = new HashMap<>();

    @CommandInfo(name="write", type=CommandInfo.Type.WRITE)
    public void write(@CommandInfo.Argument("key") String key, @CommandInfo.Argument("value") Object value) {
      data.put(key, value);
    }

    @Stateful("foo")
    public JsonObject doTakeSnapshot() {
      return new JsonObject(data);
    }

    @Stateful("foo")
    public void doInstallSnapshot(Map<String, Object> snapshot) {
      this.data = snapshot;
    }
  }

  @Test
  public void testTakeSnapshotFromNamedMethod() {
    TestSnapshotStateMachine5 stateMachine = new TestSnapshotStateMachine5();
    stateMachine.applyCommand("write", new JsonObject().putString("key", "foo").putValue("value", "bar"));
    stateMachine.applyCommand("write", new JsonObject().putString("key", "bar").putValue("value", "baz"));
    stateMachine.applyCommand("write", new JsonObject().putString("key", "baz").putValue("value", "foo"));
    JsonObject snapshot = stateMachine.createSnapshot();
    assertEquals("bar", snapshot.getObject("foo").getString("foo"));
    assertEquals("baz", snapshot.getObject("foo").getString("bar"));
    assertEquals("foo", snapshot.getObject("foo").getString("baz"));
  }

  @Test
  public void testInstallSnapshotToNamedMethod() {
    TestSnapshotStateMachine5 stateMachine = new TestSnapshotStateMachine5();
    stateMachine.installSnapshot(new JsonObject()
        .putObject("foo", new JsonObject().putString("foo", "bar")
            .putString("bar", "baz")
            .putString("baz", "foo")));
    JsonObject snapshot = stateMachine.createSnapshot();
    assertTrue(snapshot instanceof JsonObject);
    assertEquals("bar", snapshot.getObject("foo").getString("foo"));
    assertEquals("baz", snapshot.getObject("foo").getString("bar"));
    assertEquals("foo", snapshot.getObject("foo").getString("baz"));
  }

}
