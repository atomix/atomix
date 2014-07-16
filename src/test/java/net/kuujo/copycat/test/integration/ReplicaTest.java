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

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.assertTrue;
import static org.vertx.testtools.VertxAssert.fail;
import static org.vertx.testtools.VertxAssert.testComplete;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import net.kuujo.copycat.Command;
import net.kuujo.copycat.CopyCat;
import net.kuujo.copycat.SnapshotCreator;
import net.kuujo.copycat.SnapshotInstaller;
import net.kuujo.copycat.cluster.LocalClusterManager;
import net.kuujo.copycat.cluster.StaticClusterConfig;
import net.kuujo.copycat.cluster.StaticClusterManager;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.MemoryLog;
import net.kuujo.copycat.log.SnapshotEntry;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

/**
 * A file log test.
 *
 * @author Jordan Halterman
 */
public class ReplicaTest extends TestVerticle {

  @Test
  public void testStaticCopyCat() {
    final CopyCat copyCat1 = new CopyCat(vertx, new StaticClusterManager(new StaticClusterConfig().setLocalMember("foo").setRemoteMembers("bar", "baz")));
    final CopyCat copyCat2 = new CopyCat(vertx, new StaticClusterManager(new StaticClusterConfig().setLocalMember("bar").setRemoteMembers("foo", "baz")));
    final CopyCat copyCat3 = new CopyCat(vertx, new StaticClusterManager(new StaticClusterConfig().setLocalMember("baz").setRemoteMembers("foo", "bar")));

    Command<JsonObject, JsonObject> command = new Command<JsonObject, JsonObject>() {
      @Override
      public JsonObject execute(JsonObject args) {
        return args;
      }
    };

    copyCat1.registerReadCommand("foo", command);
    copyCat1.registerWriteCommand("bar", command);
    copyCat1.registerCommand("baz", command);
    copyCat2.registerReadCommand("foo", command);
    copyCat2.registerWriteCommand("bar", command);
    copyCat2.registerCommand("baz", command);
    copyCat3.registerReadCommand("foo", command);
    copyCat3.registerWriteCommand("bar", command);
    copyCat3.registerCommand("baz", command);

    final AtomicInteger counter = new AtomicInteger();
    copyCat1.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          fail(result.cause().getMessage());
        } else {
          if (counter.incrementAndGet() == 3) {
            copyCat1.submitCommand("foo", new JsonObject().putString("bar", "baz"), new Handler<AsyncResult<JsonObject>>() {
              @Override
              public void handle(AsyncResult<JsonObject> result) {
                if (result.failed()) {
                  fail(result.cause().getMessage());
                } else {
                  System.out.println(result.result());
                }
              }
            });
          }
        }
      }
    });
    copyCat2.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          fail(result.cause().getMessage());
        } else {
          if (counter.incrementAndGet() == 3) {
            copyCat2.submitCommand("bar", new JsonObject().putString("baz", "foo"), new Handler<AsyncResult<JsonObject>>() {
              @Override
              public void handle(AsyncResult<JsonObject> result) {
                if (result.failed()) {
                  fail(result.cause().getMessage());
                } else {
                  System.out.println(result.result());
                }
              }
            });
          }
        }
      }
    });
    copyCat3.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          fail(result.cause().getMessage());
        } else {
          if (counter.incrementAndGet() == 3) {
            copyCat3.submitCommand("baz", new JsonObject().putString("foo", "bar"), new Handler<AsyncResult<JsonObject>>() {
              @Override
              public void handle(AsyncResult<JsonObject> result) {
                if (result.failed()) {
                  fail(result.cause().getMessage());
                } else {
                  System.out.println(result.result());
                }
              }
            });
          }
        }
      }
    });
  }

  @Test
  public void testManagedCopyCat() {
    final CopyCat copyCat1 = new CopyCat(vertx, new LocalClusterManager("test", vertx, 2));
    final CopyCat copyCat2 = new CopyCat(vertx, new LocalClusterManager("test", vertx, 2));
    final CopyCat copyCat3 = new CopyCat(vertx, new LocalClusterManager("test", vertx, 2));

    Command<JsonObject, JsonObject> command = new Command<JsonObject, JsonObject>() {
      @Override
      public JsonObject execute(JsonObject args) {
        return args;
      }
    };

    copyCat1.registerReadCommand("foo", command);
    copyCat1.registerWriteCommand("bar", command);
    copyCat1.registerCommand("baz", command);
    copyCat2.registerReadCommand("foo", command);
    copyCat2.registerWriteCommand("bar", command);
    copyCat2.registerCommand("baz", command);
    copyCat3.registerReadCommand("foo", command);
    copyCat3.registerWriteCommand("bar", command);
    copyCat3.registerCommand("baz", command);

    final AtomicInteger counter = new AtomicInteger();
    copyCat1.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          fail(result.cause().getMessage());
        } else {
          if (counter.incrementAndGet() == 3) {
            copyCat1.submitCommand("foo", new JsonObject().putString("bar", "baz"), new Handler<AsyncResult<JsonObject>>() {
              @Override
              public void handle(AsyncResult<JsonObject> result) {
                if (result.failed()) {
                  fail(result.cause().getMessage());
                } else {
                  System.out.println(result.result());
                }
              }
            });
          }
        }
      }
    });
    copyCat2.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          fail(result.cause().getMessage());
        } else {
          if (counter.incrementAndGet() == 3) {
            copyCat2.submitCommand("bar", new JsonObject().putString("baz", "foo"), new Handler<AsyncResult<JsonObject>>() {
              @Override
              public void handle(AsyncResult<JsonObject> result) {
                if (result.failed()) {
                  fail(result.cause().getMessage());
                } else {
                  System.out.println(result.result());
                }
              }
            });
          }
        }
      }
    });
    copyCat3.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          fail(result.cause().getMessage());
        } else {
          if (counter.incrementAndGet() == 3) {
            copyCat3.submitCommand("baz", new JsonObject().putString("foo", "bar"), new Handler<AsyncResult<JsonObject>>() {
              @Override
              public void handle(AsyncResult<JsonObject> result) {
                if (result.failed()) {
                  fail(result.cause().getMessage());
                } else {
                  System.out.println(result.result());
                }
              }
            });
          }
        }
      }
    });
  }

  @Test
  public void testKeyValueStore() {
    final Map<String, Object> data = new HashMap<>();
    final CopyCat copycat = new CopyCat(vertx, new StaticClusterConfig("foo"));

    copycat.registerCommand("get", new Command<JsonObject, JsonObject>() {
      @Override
      public JsonObject execute(JsonObject args) {
        String key = args.getString("key");
        return new JsonObject().putValue("result", data.get(key));
      }
    });

    copycat.registerCommand("set", new Command<JsonObject, JsonObject>() {
      @Override
      public JsonObject execute(JsonObject args) {
        String key = args.getString("key");
        Object value = args.getValue("value");
        data.put(key, value);
        return null;
      }
    });

    copycat.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        copycat.submitCommand("set", new JsonObject().putString("key", "foo").putString("value", "Hello world!"), new Handler<AsyncResult<JsonObject>>() {
          @Override
          public void handle(AsyncResult<JsonObject> result) {
            assertTrue(result.succeeded());
            copycat.submitCommand("get", new JsonObject().putString("key", "foo"), new Handler<AsyncResult<JsonObject>>() {
              @Override
              public void handle(AsyncResult<JsonObject> result) {
                assertTrue(result.succeeded());
                assertEquals("Hello world!", result.result().getString("result"));
                testComplete();
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testCreateSnapshot() {
    final Log log = new MemoryLog();
    log.setMaxSize(10);
    final CopyCat copycat = new CopyCat(vertx, log, new StaticClusterConfig("foo"));

    copycat.registerCommand("set", new Command<JsonObject, JsonObject>() {
      @Override
      public JsonObject execute(JsonObject args) {
        return null;
      }
    });

    final AtomicBoolean created = new AtomicBoolean(false);
    copycat.snapshotCreator(new SnapshotCreator() {
      @Override
      public JsonObject createSnapshot() {
        created.set(true);
        return new JsonObject().putString("foo", "bar");
      }
    });

    copycat.snapshotInstaller(new SnapshotInstaller() {
      @Override
      public void installSnapshot(JsonObject snapshot) {
      }
    });

    copycat.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        logEntries(0, 10, copycat, new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());
            assertTrue(created.get());
            Entry entry = log.getEntry(log.firstIndex());
            assertTrue(entry instanceof SnapshotEntry);
            assertEquals("bar", ((SnapshotEntry) entry).data());
            testComplete();
          }
        });
      }
    });
  }

  private void logEntries(final int count, final int total, final CopyCat copycat, final Handler<AsyncResult<Void>> doneHandler) {
    if (count == total) {
      new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
    } else {
      copycat.submitCommand("set", new JsonObject().putString("key", "foo").putString("value", "bar"), new Handler<AsyncResult<JsonObject>>() {
        @Override
        public void handle(AsyncResult<JsonObject> result) {
          if (result.failed()) {
            new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
          } else {
            logEntries(count+1, total, copycat, doneHandler);
          }
        }
      });
    }
  }

}
