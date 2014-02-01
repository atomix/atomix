/*
 * Copyright 2013 the original author or authors.
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
package net.kuujo.copycat.test.integration;

import net.kuujo.copycat.Command;
import net.kuujo.copycat.CopyCat;
import net.kuujo.copycat.Function;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.impl.DefaultCommand;
import net.kuujo.copycat.log.CommandEntry;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.replica.Replica;
import net.kuujo.copycat.replica.impl.RaftReplica;
import net.kuujo.copycat.state.StateMachine;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import static org.vertx.testtools.VertxAssert.assertTrue;
import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.testComplete;

import org.vertx.testtools.TestVerticle;

/**
 * A service test.
 *
 * @author Jordan Halterman
 */
public class ServiceTest extends TestVerticle {
  private Object value;

  @Test
  public void testService() {
    final CopyCat copycat = new CopyCat(this);
    copycat.createService("test")
      .registerCommand("set", Command.Type.WRITE, new Function<Command<Object>, Boolean>() {
        @Override
        public Boolean call(Command<Object> command) {
          value = command.data();
          return true;
        }
      })
      .registerCommand("get", Command.Type.READ, new Function<Command<Void>, Object>() {
        @Override
        public Object call(Command<Void> command) {
          return value;
        }
      })
      .start(new Handler<AsyncResult<Void>>() {
        @Override
        public void handle(AsyncResult<Void> result) {
          assertTrue(result.succeeded());

          JsonObject json = new JsonObject().putString("command", "set").putString("data", "Hello world!");
          vertx.eventBus().send("test", json, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> message) {
              assertEquals("ok", message.body().getString("status"));
              assertTrue(message.body().getBoolean("result"));

              JsonObject json = new JsonObject().putString("command", "get");
              vertx.eventBus().send("test", json, new Handler<Message<JsonObject>>() {
                @Override
                public void handle(Message<JsonObject> message) {
                  assertEquals("ok", message.body().getString("status"));
                  assertEquals("Hello world!", message.body().getString("result"));
                }
              });
            }
          });

        }
    });
  }

}
