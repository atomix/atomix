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

import net.kuujo.copycat.Replica;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.annotations.Command;
import net.kuujo.copycat.impl.DefaultReplica;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.platform.Verticle;

/**
 * A key-value store.
 *
 * @author Jordan Halterman
 */
public class KeyValueStore extends Verticle implements StateMachine {
  private final Map<String, Object> data = new HashMap<>();

  @Command(name="get", type=Command.Type.READ)
  public Object get(@Command.Argument("key") String key, @Command.Argument(value="default", required=false) Object defaultValue) {
    return data.containsKey(key) ? data.get(key) : defaultValue;
  }

  @Command(name="set", type=Command.Type.WRITE)
  public boolean set(@Command.Argument("key") String key, @Command.Argument("value") Object value) {
    data.put(key, value);
    return true;
  }

  @Command(name="del", type=Command.Type.WRITE)
  public boolean del(@Command.Argument("key") String key) {
    if (data.containsKey(key)) {
      data.remove(key);
      return true;
    }
    return false;
  }

  @Override
  public void start(final Future<Void> startResult) {
    String address = container.config().getString("address", "copycat");
    Replica replica = new DefaultReplica(address, vertx, this);
    replica.start(new Handler<AsyncResult<Void>>() {
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
