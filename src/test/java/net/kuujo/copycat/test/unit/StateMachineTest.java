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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.annotations.Command;
import net.kuujo.copycat.annotations.Stateful;
import net.kuujo.copycat.impl.DefaultStateMachineExecutor;

import org.junit.Test;
import org.vertx.java.core.json.JsonElement;
import org.vertx.java.core.json.JsonObject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * State machine tests.
 *
 * @author Jordan Halterman
 */
public class StateMachineTest {

  public static class TestStateMachine implements StateMachine {

    @Command(name="one")
    public String one() {
      return "one";
    }

    @Command(name="two")
    public Object two(@Command.Value JsonObject args) {
      return args.getString("foo");
    }

    @Command(name="three")
    public Boolean three(String arg0, Boolean arg1) {
      return arg1;
    }

    @Command(name="four", type=Command.Type.WRITE)
    public String four(@Command.Argument("foo") String foo) {
      return foo;
    }

    @Command(name="five", type=Command.Type.READ)
    public String five(@Command.Argument("foo") String foo, @Command.Argument("bar") Boolean bar) {
      return foo;
    }

    @Command(name="six", type=Command.Type.READ_WRITE)
    public Object six(@Command.Argument(value="foo", required=false) String foo) {
      return foo;
    }

    @Command(name="seven", type=Command.Type.READ_WRITE)
    public Object seven(@Command.Argument("foo") String foo, @Command.Argument("bar") Boolean bar, @Command.Argument(value="baz", required=false) Object baz) {
      return foo;
    }

  }

  @Test
  public void testFindCommands() {
    DefaultStateMachineExecutor adapter = new DefaultStateMachineExecutor(new TestStateMachine());
    Collection<Command> commands = adapter.getCommands();
    assertTrue(commands.size() == 7);
  }

  @Test
  public void testNoArgCommand() {
    DefaultStateMachineExecutor adapter = new DefaultStateMachineExecutor(new TestStateMachine());
    Command command = adapter.getCommand("one");
    assertNotNull(command);
    assertEquals("one", command.name());
    assertEquals(Command.Type.READ_WRITE, command.type());
    assertEquals("one", adapter.applyCommand("one", new JsonObject()));
  }

  @Test
  public void testDefaultArgCommand() {
    DefaultStateMachineExecutor adapter = new DefaultStateMachineExecutor(new TestStateMachine());
    Command command = adapter.getCommand("two");
    assertNotNull(command);
    assertEquals("two", command.name());
    assertEquals(Command.Type.READ_WRITE, command.type());
    assertEquals("bar", adapter.applyCommand("two", new JsonObject().putString("foo", "bar")));
  }

  @Test
  public void testAutoNamedArgsCommand() {
    DefaultStateMachineExecutor adapter = new DefaultStateMachineExecutor(new TestStateMachine());
    Command command = adapter.getCommand("three");
    assertNotNull(command);
    assertEquals("three", command.name());
    assertEquals(Command.Type.READ_WRITE, command.type());
    assertEquals(true, adapter.applyCommand("three", new JsonObject().putString("arg0", "Hello world!").putBoolean("arg1", true)));
  }

  @Test
  public void testAutoNamedArgsCommandFail() {
    DefaultStateMachineExecutor adapter = new DefaultStateMachineExecutor(new TestStateMachine());
    Command command = adapter.getCommand("three");
    assertNotNull(command);
    assertEquals("three", command.name());
    assertEquals(Command.Type.READ_WRITE, command.type());
    try {
      adapter.applyCommand("three", new JsonObject().putString("arg0", "Hello world!"));
      fail("Failed to prevent application.");
    }
    catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testNamedArgCommand() {
    DefaultStateMachineExecutor adapter = new DefaultStateMachineExecutor(new TestStateMachine());
    Command command = adapter.getCommand("four");
    assertNotNull(command);
    assertEquals("four", command.name());
    assertEquals(Command.Type.WRITE, command.type());
    assertEquals("Hello world!", adapter.applyCommand("four", new JsonObject().putString("foo", "Hello world!")));
  }

  @Test
  public void testNamedArgCommandFail() {
    DefaultStateMachineExecutor adapter = new DefaultStateMachineExecutor(new TestStateMachine());
    Command command = adapter.getCommand("four");
    assertNotNull(command);
    assertEquals("four", command.name());
    assertEquals(Command.Type.WRITE, command.type());
    try {
      adapter.applyCommand("four", new JsonObject());
      fail("Failed to prevent application.");
    }
    catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testNamedArgsCommand() {
    DefaultStateMachineExecutor adapter = new DefaultStateMachineExecutor(new TestStateMachine());
    Command command = adapter.getCommand("five");
    assertNotNull(command);
    assertEquals("five", command.name());
    assertEquals(Command.Type.READ, command.type());
    assertEquals("Hello world!", adapter.applyCommand("five", new JsonObject().putString("foo", "Hello world!").putBoolean("bar", true)));
  }

  @Test
  public void testNamedArgsCommandFail() {
    DefaultStateMachineExecutor adapter = new DefaultStateMachineExecutor(new TestStateMachine());
    Command command = adapter.getCommand("five");
    assertNotNull(command);
    assertEquals("five", command.name());
    assertEquals(Command.Type.READ, command.type());
    try {
      adapter.applyCommand("five", new JsonObject().putBoolean("bar", true));
      fail("Failed to prevent application.");
    }
    catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testOptionalArgCommand() {
    DefaultStateMachineExecutor adapter = new DefaultStateMachineExecutor(new TestStateMachine());
    Command command = adapter.getCommand("six");
    assertNotNull(command);
    assertEquals("six", command.name());
    assertEquals(Command.Type.READ_WRITE, command.type());
    assertEquals("Hello world!", adapter.applyCommand("six", new JsonObject().putString("foo", "Hello world!")));
    assertNull(adapter.applyCommand("six", new JsonObject().putString("something", "else")));
  }

  @Test
  public void testOptionalArgsCommand() {
    DefaultStateMachineExecutor adapter = new DefaultStateMachineExecutor(new TestStateMachine());
    Command command = adapter.getCommand("seven");
    assertNotNull(command);
    assertEquals("seven", command.name());
    assertEquals(Command.Type.READ_WRITE, command.type());
    assertEquals("Hello world!", adapter.applyCommand("seven", new JsonObject().putString("foo", "Hello world!").putBoolean("bar", true).putString("baz", "Something else")));
    assertEquals("Hello world!", adapter.applyCommand("seven", new JsonObject().putString("foo", "Hello world!").putBoolean("bar", true)));
  }

  public static class TestSnapshotStateMachine implements StateMachine {
    @Stateful
    private final Map<String, Object> data = new HashMap<>();

    @Command(name="write", type=Command.Type.WRITE)
    public void write(@Command.Argument("key") String key, @Command.Argument("value") Object value) {
      data.put(key, value);
    }
  }

  @Test
  public void testTakeSnapshotFromField() {
    DefaultStateMachineExecutor adapter = new DefaultStateMachineExecutor(new TestSnapshotStateMachine());
    adapter.applyCommand("write", new JsonObject().putString("key", "foo").putValue("value", "bar"));
    adapter.applyCommand("write", new JsonObject().putString("key", "bar").putValue("value", "baz"));
    adapter.applyCommand("write", new JsonObject().putString("key", "baz").putValue("value", "foo"));
    JsonElement snapshot = adapter.takeSnapshot();
    assertTrue(snapshot instanceof JsonObject);
    assertEquals("bar", snapshot.asObject().getObject("data").getString("foo"));
    assertEquals("baz", snapshot.asObject().getObject("data").getString("bar"));
    assertEquals("foo", snapshot.asObject().getObject("data").getString("baz"));
  }

  @Test
  public void testInstallSnapshotToField() {
    DefaultStateMachineExecutor adapter = new DefaultStateMachineExecutor(new TestSnapshotStateMachine());
    adapter.installSnapshot(new JsonObject()
        .putObject("data", new JsonObject()
        .putString("foo", "bar")
        .putString("bar", "baz")
        .putString("baz", "foo")));
    JsonElement snapshot = adapter.takeSnapshot();
    assertTrue(snapshot instanceof JsonObject);
    assertEquals("bar", snapshot.asObject().getObject("data").getString("foo"));
    assertEquals("baz", snapshot.asObject().getObject("data").getString("bar"));
    assertEquals("foo", snapshot.asObject().getObject("data").getString("baz"));
  }

  public static class TestSnapshotStateMachine2 implements StateMachine {
    private Map<String, Object> data = new HashMap<>();

    @Command(name="write", type=Command.Type.WRITE)
    public void write(@Command.Argument("key") String key, @Command.Argument("value") Object value) {
      data.put(key, value);
    }

    @Stateful("data")
    public Map<String, Object> takeSnapshot() {
      return data;
    }

    @Stateful("data")
    public void installSnapshot(Map<String, Object> snapshot) {
      this.data = snapshot;
    }
  }

  @Test
  public void testTakeSnapshotFromMethod() {
    DefaultStateMachineExecutor adapter = new DefaultStateMachineExecutor(new TestSnapshotStateMachine2());
    adapter.applyCommand("write", new JsonObject().putString("key", "foo").putValue("value", "bar"));
    adapter.applyCommand("write", new JsonObject().putString("key", "bar").putValue("value", "baz"));
    adapter.applyCommand("write", new JsonObject().putString("key", "baz").putValue("value", "foo"));
    JsonElement snapshot = adapter.takeSnapshot();
    assertEquals("bar", snapshot.asObject().getObject("data").getString("foo"));
    assertEquals("baz", snapshot.asObject().getObject("data").getString("bar"));
    assertEquals("foo", snapshot.asObject().getObject("data").getString("baz"));
  }

  @Test
  public void testInstallSnapshotToMethod() {
    DefaultStateMachineExecutor adapter = new DefaultStateMachineExecutor(new TestSnapshotStateMachine2());
    adapter.installSnapshot(new JsonObject()
        .putObject("data", new JsonObject()
            .putString("foo", "bar")
            .putString("bar", "baz")
            .putString("baz", "foo")));
    JsonElement snapshot = adapter.takeSnapshot();
    assertTrue(snapshot instanceof JsonObject);
    assertEquals("bar", snapshot.asObject().getObject("data").getString("foo"));
    assertEquals("baz", snapshot.asObject().getObject("data").getString("bar"));
    assertEquals("foo", snapshot.asObject().getObject("data").getString("baz"));
  }

  public static class TestSnapshotStateMachine3 implements StateMachine {
    @Stateful
    private Map<String, Object> data = new HashMap<>();

    @Command(name="write", type=Command.Type.WRITE)
    public void write(@Command.Argument("key") String key, @Command.Argument("value") Object value) {
      data.put(key, value);
    }

    public Map<String, Object> takeSnapshot() {
      return data;
    }

    public void installSnapshot(Map<String, Object> snapshot) {
      this.data = snapshot;
    }
  }

  @Test
  public void testTakeSnapshotFromDetectedMethod() {
    DefaultStateMachineExecutor adapter = new DefaultStateMachineExecutor(new TestSnapshotStateMachine3());
    adapter.applyCommand("write", new JsonObject().putString("key", "foo").putValue("value", "bar"));
    adapter.applyCommand("write", new JsonObject().putString("key", "bar").putValue("value", "baz"));
    adapter.applyCommand("write", new JsonObject().putString("key", "baz").putValue("value", "foo"));
    JsonElement snapshot = adapter.takeSnapshot();
    assertEquals("bar", snapshot.asObject().getObject("data").getString("foo"));
    assertEquals("baz", snapshot.asObject().getObject("data").getString("bar"));
    assertEquals("foo", snapshot.asObject().getObject("data").getString("baz"));
  }

  @Test
  public void testInstallSnapshotToDetectedMethod() {
    DefaultStateMachineExecutor adapter = new DefaultStateMachineExecutor(new TestSnapshotStateMachine3());
    adapter.installSnapshot(new JsonObject()
        .putObject("data", new JsonObject().putString("foo", "bar")
            .putString("bar", "baz")
            .putString("baz", "foo")));
    JsonElement snapshot = adapter.takeSnapshot();
    assertTrue(snapshot instanceof JsonObject);
    assertEquals("bar", snapshot.asObject().getObject("data").getString("foo"));
    assertEquals("baz", snapshot.asObject().getObject("data").getString("bar"));
    assertEquals("foo", snapshot.asObject().getObject("data").getString("baz"));
  }

  public static class TestSnapshotStateMachine4 implements StateMachine {
    @Stateful("foo")
    private Map<String, Object> data = new HashMap<>();

    @Command(name="write", type=Command.Type.WRITE)
    public void write(@Command.Argument("key") String key, @Command.Argument("value") Object value) {
      data.put(key, value);
    }

    public Map<String, Object> takeSnapshot() {
      return data;
    }

    public void installSnapshot(Map<String, Object> snapshot) {
      this.data = snapshot;
    }
  }

  @Test
  public void testTakeSnapshotFromNamedField() {
    DefaultStateMachineExecutor adapter = new DefaultStateMachineExecutor(new TestSnapshotStateMachine4());
    adapter.applyCommand("write", new JsonObject().putString("key", "foo").putValue("value", "bar"));
    adapter.applyCommand("write", new JsonObject().putString("key", "bar").putValue("value", "baz"));
    adapter.applyCommand("write", new JsonObject().putString("key", "baz").putValue("value", "foo"));
    JsonElement snapshot = adapter.takeSnapshot();
    assertEquals("bar", snapshot.asObject().getObject("foo").getString("foo"));
    assertEquals("baz", snapshot.asObject().getObject("foo").getString("bar"));
    assertEquals("foo", snapshot.asObject().getObject("foo").getString("baz"));
  }

  @Test
  public void testInstallSnapshotToNamedField() {
    DefaultStateMachineExecutor adapter = new DefaultStateMachineExecutor(new TestSnapshotStateMachine4());
    adapter.installSnapshot(new JsonObject()
        .putObject("foo", new JsonObject()
            .putString("foo", "bar")
            .putString("bar", "baz")
            .putString("baz", "foo")));
    JsonElement snapshot = adapter.takeSnapshot();
    assertTrue(snapshot instanceof JsonObject);
    assertEquals("bar", snapshot.asObject().getObject("foo").getString("foo"));
    assertEquals("baz", snapshot.asObject().getObject("foo").getString("bar"));
    assertEquals("foo", snapshot.asObject().getObject("foo").getString("baz"));
  }

  public static class TestSnapshotStateMachine5 implements StateMachine {
    @Stateful("foo")
    private Map<String, Object> data = new HashMap<>();

    @Command(name="write", type=Command.Type.WRITE)
    public void write(@Command.Argument("key") String key, @Command.Argument("value") Object value) {
      data.put(key, value);
    }

    @Stateful("foo")
    public Map<String, Object> takeSnapshot() {
      return data;
    }

    @Stateful("foo")
    public void installSnapshot(Map<String, Object> snapshot) {
      this.data = snapshot;
    }
  }

  @Test
  public void testTakeSnapshotFromNamedMethod() {
    DefaultStateMachineExecutor adapter = new DefaultStateMachineExecutor(new TestSnapshotStateMachine5());
    adapter.applyCommand("write", new JsonObject().putString("key", "foo").putValue("value", "bar"));
    adapter.applyCommand("write", new JsonObject().putString("key", "bar").putValue("value", "baz"));
    adapter.applyCommand("write", new JsonObject().putString("key", "baz").putValue("value", "foo"));
    JsonElement snapshot = adapter.takeSnapshot();
    assertEquals("bar", snapshot.asObject().getObject("foo").getString("foo"));
    assertEquals("baz", snapshot.asObject().getObject("foo").getString("bar"));
    assertEquals("foo", snapshot.asObject().getObject("foo").getString("baz"));
  }

  @Test
  public void testInstallSnapshotToNamedMethod() {
    DefaultStateMachineExecutor adapter = new DefaultStateMachineExecutor(new TestSnapshotStateMachine5());
    adapter.installSnapshot(new JsonObject()
        .putObject("foo", new JsonObject().putString("foo", "bar")
            .putString("bar", "baz")
            .putString("baz", "foo")));
    JsonElement snapshot = adapter.takeSnapshot();
    assertTrue(snapshot instanceof JsonObject);
    assertEquals("bar", snapshot.asObject().getObject("foo").getString("foo"));
    assertEquals("baz", snapshot.asObject().getObject("foo").getString("bar"));
    assertEquals("foo", snapshot.asObject().getObject("foo").getString("baz"));
  }

}
