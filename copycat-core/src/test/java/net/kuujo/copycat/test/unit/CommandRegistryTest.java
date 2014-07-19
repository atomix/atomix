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
import net.kuujo.copycat.CommandInfo;
import net.kuujo.copycat.vertx.Command;
import net.kuujo.copycat.vertx.CommandRegistry;
import net.kuujo.copycat.vertx.SnapshotCreator;
import net.kuujo.copycat.vertx.SnapshotInstaller;

import org.junit.Test;
import org.vertx.java.core.json.JsonObject;

/**
 * Command registry tests.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CommandRegistryTest {

  @Test
  public void testRegisterCommand() {
    CommandRegistry registry = new CommandRegistry();
    registry.registerCommand("foo", new Command<JsonObject, JsonObject>() {
      @Override
      public JsonObject execute(JsonObject args) {
        return args;
      }
    });
    CommandInfo info = registry.getCommandInfo("foo");
    assertEquals("foo", info.name());
    assertEquals(CommandInfo.Type.READ_WRITE, info.type());
  }

  @Test
  public void testRegisterTypedCommand() {
    CommandRegistry registry = new CommandRegistry();
    registry.registerCommand("foo", CommandInfo.Type.READ, new Command<JsonObject, JsonObject>() {
      @Override
      public JsonObject execute(JsonObject args) {
        return args;
      }
    });
    CommandInfo info = registry.getCommandInfo("foo");
    assertEquals("foo", info.name());
    assertEquals(CommandInfo.Type.READ, info.type());
  }

  @Test
  public void testRegisterSnapshotCreator() {
    CommandRegistry registry = new CommandRegistry();
    SnapshotCreator creator = new SnapshotCreator() {
      @Override
      public JsonObject createSnapshot() {
        return new JsonObject();
      }
    };
    registry.snapshotCreator(creator);
    assertEquals(creator, registry.snapshotCreator());
  }

  @Test
  public void testRegisterSnapshotInstaller() {
    CommandRegistry registry = new CommandRegistry();
    SnapshotInstaller installer = new SnapshotInstaller() {
      @Override
      public void installSnapshot(JsonObject snapshot) {
      }
    };
    registry.snapshotInstaller(installer);
    assertEquals(installer, registry.snapshotInstaller());
  }

}
