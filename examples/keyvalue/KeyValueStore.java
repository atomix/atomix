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
import java.util.HashMap;
import java.util.Map;

import net.kuujo.copycat.Command;
import net.kuujo.copycat.CopyCat;
import net.kuujo.copycat.CopyCatService;
import net.kuujo.copycat.Function;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

/**
 * A key-value store.
 *
 * @author Jordan Halterman
 */
public class KeyValueStore extends Verticle {
  private CopyCat copycat;
  private CopyCatService service;
  private final Map<String, Object> data = new HashMap<>();
  private final Map<String, Command<?>> commands = new HashMap<>();

  private final Function<Command<JsonObject>, Object> get = new Function<Command<JsonObject>, Object>() {
    @Override
    public Object call(Command<JsonObject> command) {
      String key = command.data().getString("key");
      return data.containsKey(key) ? data.get(key) : null;
    }
  };

  private final Function<Command<JsonObject>, Boolean> set = new Function<Command<JsonObject>, Boolean>() {
    @Override
    public Boolean call(Command<JsonObject> command) {
      String key = command.data().getString("key");
      Object value = command.data().getValue("value");
      data.put(key, value);
      if (commands.containsKey(key)) {
        commands.remove(key).free();
      }
      commands.put(key, command);
      return true;
    }
  };

  private final Function<Command<JsonObject>, Boolean> del = new Function<Command<JsonObject>, Boolean>() {
    @Override
    public Boolean call(Command<JsonObject> command) {
      String key = command.data().getString("key");
      if (commands.containsKey(key)) {
        commands.remove(key).free();
      }
      if (data.containsKey(key)) {
        data.remove(key);
        return true;
      }
      return false;
    }
  };

  @Override
  public void start(final Future<Void> startResult) {
    String address = container.config().getString("address", "copyredis");
    copycat = new CopyCat(this);
    service = copycat.createService(address);

    service.registerCommand("get", Command.Type.READ, get);
    service.registerCommand("set", Command.Type.WRITE, set);
    service.registerCommand("del", Command.Type.WRITE, del);

    service.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          startResult.setFailure(result.cause());
        }
        else {
          KeyValueStore.super.start(startResult);
        }
      }
    });
  }

}
