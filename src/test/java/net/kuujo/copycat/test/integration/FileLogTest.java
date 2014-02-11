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

import net.kuujo.copycat.log.CommandEntry;
import net.kuujo.copycat.log.ConfigurationEntry;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.LogProxy;
import net.kuujo.copycat.log.NoOpEntry;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;

import static org.vertx.testtools.VertxAssert.assertTrue;
import static org.vertx.testtools.VertxAssert.fail;
import static org.vertx.testtools.VertxAssert.testComplete;

import org.vertx.testtools.TestVerticle;

/**
 * A file log test.
 *
 * @author Jordan Halterman
 */
public class FileLogTest extends TestVerticle {

  @Test
  public void testAppendEntry() {
    final LogProxy log = new LogProxy("test", vertx, container);
    log.setLogType(Log.Type.FILE);
    log.open(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          fail(result.cause().getMessage());
        }
        else {
          log.appendEntry(new NoOpEntry(), new Handler<AsyncResult<Long>>() {
            @Override
            public void handle(AsyncResult<Long> result) {
              assertTrue(result.succeeded());
              assertTrue(result.result() == 1);
              completeTest(log);
            }
          });
        }
      }
    });
  }

  @Test
  public void testContainsEntry() {
    final LogProxy log = new LogProxy("test", vertx, container);
    log.setLogType(Log.Type.FILE);
    log.open(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          fail(result.cause().getMessage());
        }
        else {
          log.appendEntry(new NoOpEntry(), new Handler<AsyncResult<Long>>() {
            @Override
            public void handle(AsyncResult<Long> result) {
              assertTrue(result.succeeded());
              assertTrue(result.result() == 1);
              log.containsEntry(1, new Handler<AsyncResult<Boolean>>() {
                @Override
                public void handle(AsyncResult<Boolean> result) {
                  assertTrue(result.succeeded());
                  assertTrue(result.result());
                  completeTest(log);
                }
              });
            }
          });
        }
      }
    });
  }

  @Test
  public void testLoadEntry() {
    final LogProxy log = new LogProxy("test", vertx, container);
    log.setLogType(Log.Type.FILE);
    log.open(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          fail(result.cause().getMessage());
        }
        else {
          log.appendEntry(new NoOpEntry(), new Handler<AsyncResult<Long>>() {
            @Override
            public void handle(AsyncResult<Long> result) {
              assertTrue(result.succeeded());
              assertTrue(result.result() == 1);
              log.getEntry(1, new Handler<AsyncResult<Entry>>() {
                @Override
                public void handle(AsyncResult<Entry> result) {
                  assertTrue(result.succeeded());
                  assertTrue(result.result() instanceof NoOpEntry);
                  completeTest(log);
                }
              });
            }
          });
        }
      }
    });
  }

  @Test
  public void testFirstIndex() {
    final LogProxy log = new LogProxy("test", vertx, container);
    log.setLogType(Log.Type.FILE);
    log.open(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          fail(result.cause().getMessage());
        }
        else {
          log.appendEntry(new NoOpEntry(), new Handler<AsyncResult<Long>>() {
            @Override
            public void handle(AsyncResult<Long> result) {
              assertTrue(result.succeeded());
              assertTrue(result.result() == 1);

              log.appendEntry(new ConfigurationEntry(), new Handler<AsyncResult<Long>>() {
                @Override
                public void handle(AsyncResult<Long> result) {
                  assertTrue(result.succeeded());
                  assertTrue(result.result() == 2);

                  log.appendEntry(new CommandEntry(1, "foo", new JsonObject()), new Handler<AsyncResult<Long>>() {
                    @Override
                    public void handle(AsyncResult<Long> result) {
                      assertTrue(result.succeeded());
                      assertTrue(result.result() == 3);
                      log.firstIndex(new Handler<AsyncResult<Long>>() {
                        @Override
                        public void handle(AsyncResult<Long> result) {
                          assertTrue(result.succeeded());
                          assertTrue(result.result() == 1);
                        }
                      });
                      log.lastIndex(new Handler<AsyncResult<Long>>() {
                        @Override
                        public void handle(AsyncResult<Long> result) {
                          assertTrue(result.succeeded());
                          assertTrue(result.result() == 3);
                          completeTest(log);
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
    });
  }

  @Test
  public void testFirstEntry() {
    final LogProxy log = new LogProxy("test", vertx, container);
    log.setLogType(Log.Type.FILE);
    log.open(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          fail(result.cause().getMessage());
        }
        else {
          log.appendEntry(new NoOpEntry(), new Handler<AsyncResult<Long>>() {
            @Override
            public void handle(AsyncResult<Long> result) {
              assertTrue(result.succeeded());
              assertTrue(result.result() == 1);

              log.appendEntry(new ConfigurationEntry(), new Handler<AsyncResult<Long>>() {
                @Override
                public void handle(AsyncResult<Long> result) {
                  assertTrue(result.succeeded());
                  assertTrue(result.result() == 2);

                  log.appendEntry(new CommandEntry(1, "foo", new JsonObject()), new Handler<AsyncResult<Long>>() {
                    @Override
                    public void handle(AsyncResult<Long> result) {
                      assertTrue(result.succeeded());
                      assertTrue(result.result() == 3);

                      log.firstEntry(new Handler<AsyncResult<Entry>>() {
                        @Override
                        public void handle(AsyncResult<Entry> result) {
                          assertTrue(result.succeeded());
                          assertTrue(result.result() instanceof NoOpEntry);
                          completeTest(log);
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
    });
  }

  @Test
  public void testLastIndex() {
    final LogProxy log = new LogProxy("test", vertx, container);
    log.setLogType(Log.Type.FILE);
    log.open(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          fail(result.cause().getMessage());
        }
        else {
          log.appendEntry(new NoOpEntry(), new Handler<AsyncResult<Long>>() {
            @Override
            public void handle(AsyncResult<Long> result) {
              assertTrue(result.succeeded());
              assertTrue(result.result() == 1);

              log.appendEntry(new ConfigurationEntry(), new Handler<AsyncResult<Long>>() {
                @Override
                public void handle(AsyncResult<Long> result) {
                  assertTrue(result.succeeded());
                  assertTrue(result.result() == 2);

                  log.appendEntry(new CommandEntry(1, "foo", new JsonObject()), new Handler<AsyncResult<Long>>() {
                    @Override
                    public void handle(AsyncResult<Long> result) {
                      assertTrue(result.succeeded());
                      assertTrue(result.result() == 3);
                      log.lastIndex(new Handler<AsyncResult<Long>>() {
                        @Override
                        public void handle(AsyncResult<Long> result) {
                          assertTrue(result.succeeded());
                          assertTrue(result.result() == 3);
                          completeTest(log);
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
    });
  }

  @Test
  public void testLastEntry() {
    final LogProxy log = new LogProxy("test", vertx, container);
    log.setLogType(Log.Type.FILE);
    log.open(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          fail(result.cause().getMessage());
        }
        else {
          log.appendEntry(new NoOpEntry(), new Handler<AsyncResult<Long>>() {
            @Override
            public void handle(AsyncResult<Long> result) {
              assertTrue(result.succeeded());
              assertTrue(result.result() == 1);

              log.appendEntry(new ConfigurationEntry(), new Handler<AsyncResult<Long>>() {
                @Override
                public void handle(AsyncResult<Long> result) {
                  assertTrue(result.succeeded());
                  assertTrue(result.result() == 2);

                  log.appendEntry(new CommandEntry(1, "foo", new JsonObject()), new Handler<AsyncResult<Long>>() {
                    @Override
                    public void handle(AsyncResult<Long> result) {
                      assertTrue(result.succeeded());
                      assertTrue(result.result() == 3);

                      log.lastEntry(new Handler<AsyncResult<Entry>>() {
                        @Override
                        public void handle(AsyncResult<Entry> result) {
                          assertTrue(result.succeeded());
                          assertTrue(result.result() instanceof CommandEntry);
                          completeTest(log);
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
    });
  }

  private void completeTest(LogProxy log) {
    log.removeAfter(0, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          fail(result.cause().getMessage());
        }
        else {
          testComplete();
        }
      }
    });
  }

}
