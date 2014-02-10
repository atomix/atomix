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

import net.kuujo.copycat.log.CommandEntry;
import net.kuujo.copycat.log.ConfigurationEntry;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.MemoryLog;
import net.kuujo.copycat.log.NoOpEntry;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;

import static org.junit.Assert.assertTrue;

/**
 * In-memory log tests.
 *
 * @author Jordan Halterman
 */
public class MemoryLogTest {

  @Test
  public void testAppendEntry() {
    Log log = new MemoryLog();
    log.appendEntry(new NoOpEntry(), new Handler<AsyncResult<Long>>() {
      @Override
      public void handle(AsyncResult<Long> result) {
        assertTrue(result.succeeded());
        assertTrue(result.result() == 1);
      }
    });
  }

  @Test
  public void testContainsEntry() {
    final Log log = new MemoryLog();
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
          }
        });
      }
    });
  }

  @Test
  public void testLoadEntry() {
    final Log log = new MemoryLog();
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
          }
        });
      }
    });
  }

  @Test
  public void testFirstIndex() {
    final Log log = new MemoryLog();
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
                assertTrue(log.firstIndex() == 1);
                assertTrue(log.lastIndex() == 3);
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testFirstEntry() {
    final Log log = new MemoryLog();
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
  public void testLastIndex() {
    final Log log = new MemoryLog();
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
                assertTrue(log.lastIndex() == 3);
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testLastEntry() {
    final Log log = new MemoryLog();
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
