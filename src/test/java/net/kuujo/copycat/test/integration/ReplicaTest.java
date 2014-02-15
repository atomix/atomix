/*
09 * Copyright 2014 the original author or authors.
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

import net.kuujo.copycat.ClusterConfig;
import net.kuujo.copycat.Replica;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.annotations.Command;
import net.kuujo.copycat.annotations.Command.Argument;
import net.kuujo.copycat.impl.DefaultReplica;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;

import static org.vertx.testtools.VertxAssert.assertTrue;
import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.testComplete;

import org.vertx.testtools.TestVerticle;

/**
 * A file log test.
 *
 * @author Jordan Halterman
 */
public class ReplicaTest extends TestVerticle implements StateMachine {

  @Command(type=Command.Type.WRITE)
  public String write(@Argument("foo") String foo) {
    return foo;
  }

  @Command(type=Command.Type.READ)
  public String read() {
    return "Hello world!";
  }

  @Test
  public void testSubmitWriteCommand() {
    final ClusterConfig config = new ClusterConfig("test.1", "test.2", "test.3");
    final Replica replica1 = new DefaultReplica("test.1", vertx, container, this, config);
    replica1.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        final Replica replica2 = new DefaultReplica("test.2", vertx, container, ReplicaTest.this, config);
        replica2.start(new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());
            final Replica replica3 = new DefaultReplica("test.3", vertx, container, ReplicaTest.this, config);
            replica3.start(new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                assertTrue(result.succeeded());

                replica3.submitCommand("write", new JsonObject().putString("foo", "bar"), new Handler<AsyncResult<String>>() {
                  @Override
                  public void handle(AsyncResult<String> result) {
                    assertTrue(result.succeeded());
                    assertEquals("bar", result.result());
                    testComplete();
                  }
                });
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testSubmitReadCommand() {
    final ClusterConfig config = new ClusterConfig("test.1", "test.2", "test.3");
    final Replica replica1 = new DefaultReplica("test.1", vertx, container, this, config);
    replica1.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        final Replica replica2 = new DefaultReplica("test.2", vertx, container, ReplicaTest.this, config);
        replica2.start(new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());
            final Replica replica3 = new DefaultReplica("test.3", vertx, container, ReplicaTest.this, config);
            replica3.start(new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                assertTrue(result.succeeded());

                replica3.submitCommand("read", new JsonObject(), new Handler<AsyncResult<String>>() {
                  @Override
                  public void handle(AsyncResult<String> result) {
                    assertTrue(result.succeeded());
                    assertEquals("Hello world!", result.result());
                    testComplete();
                  }
                });
              }
            });
          }
        });
      }
    });
  }

}
