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
package net.kuujo.mimeo.test.unit;

import net.kuujo.mimeo.Command;
import net.kuujo.mimeo.impl.DefaultCommand;
import net.kuujo.mimeo.log.CommandEntry;
import net.kuujo.mimeo.log.ConfigurationEntry;
import net.kuujo.mimeo.log.Entry;
import net.kuujo.mimeo.log.Log;
import net.kuujo.mimeo.log.MemoryLog;
import net.kuujo.mimeo.log.NoOpEntry;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;

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
        assertTrue(result.result() == 0);
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
        assertTrue(result.result() == 0);
        log.containsEntry(0, new Handler<AsyncResult<Boolean>>() {
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
        assertTrue(result.result() == 0);
        log.entry(0, new Handler<AsyncResult<Entry>>() {
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
        assertTrue(result.result() == 0);

        log.appendEntry(new ConfigurationEntry(), new Handler<AsyncResult<Long>>() {
          @Override
          public void handle(AsyncResult<Long> result) {
            assertTrue(result.succeeded());
            assertTrue(result.result() == 1);

            log.appendEntry(new CommandEntry(1, new DefaultCommand<Object>()), new Handler<AsyncResult<Long>>() {
              @Override
              public void handle(AsyncResult<Long> result) {
                assertTrue(result.succeeded());
                assertTrue(result.result() == 2);

                log.firstIndex(new Handler<AsyncResult<Long>>() {
                  @Override
                  public void handle(AsyncResult<Long> result) {
                    assertTrue(result.succeeded());
                    assertTrue(result.result() == 0);
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
  public void testFreeEntry() {
    final Log log = new MemoryLog();
    final CommandEntry entry1 = new CommandEntry(1, new DefaultCommand<Integer>("foo", Command.Type.WRITE, 1));
    log.appendEntry(entry1, new Handler<AsyncResult<Long>>() {
      @Override
      public void handle(AsyncResult<Long> result) {
        assertTrue(result.succeeded());
        assertTrue(result.result() == 0);

        final CommandEntry entry2 = new CommandEntry(1, new DefaultCommand<Integer>("bar", Command.Type.WRITE, 2));
        log.appendEntry(entry2, new Handler<AsyncResult<Long>>() {
          @Override
          public void handle(AsyncResult<Long> result) {
            assertTrue(result.succeeded());
            assertTrue(result.result() == 1);

            final CommandEntry entry3 = new CommandEntry(1, new DefaultCommand<Integer>("baz", Command.Type.WRITE, 3));
            log.appendEntry(entry3, new Handler<AsyncResult<Long>>() {
              @Override
              public void handle(AsyncResult<Long> result) {
                assertTrue(result.succeeded());
                assertTrue(result.result() == 2);

                log.floor(2, new Handler<AsyncResult<Void>>() {
                  @Override
                  public void handle(AsyncResult<Void> result) {
                    assertTrue(result.succeeded());
                    log.free(entry2);

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
                        assertTrue(result.result() == 2);
                      }
                    });
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
  public void testFirstEntry() {
    final Log log = new MemoryLog();
    log.appendEntry(new NoOpEntry(), new Handler<AsyncResult<Long>>() {
      @Override
      public void handle(AsyncResult<Long> result) {
        assertTrue(result.succeeded());
        assertTrue(result.result() == 0);

        log.appendEntry(new ConfigurationEntry(), new Handler<AsyncResult<Long>>() {
          @Override
          public void handle(AsyncResult<Long> result) {
            assertTrue(result.succeeded());
            assertTrue(result.result() == 1);

            log.appendEntry(new CommandEntry(1, new DefaultCommand<Object>()), new Handler<AsyncResult<Long>>() {
              @Override
              public void handle(AsyncResult<Long> result) {
                assertTrue(result.succeeded());
                assertTrue(result.result() == 2);

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
        assertTrue(result.result() == 0);

        log.appendEntry(new ConfigurationEntry(), new Handler<AsyncResult<Long>>() {
          @Override
          public void handle(AsyncResult<Long> result) {
            assertTrue(result.succeeded());
            assertTrue(result.result() == 1);

            log.appendEntry(new CommandEntry(1, new DefaultCommand<Object>()), new Handler<AsyncResult<Long>>() {
              @Override
              public void handle(AsyncResult<Long> result) {
                assertTrue(result.succeeded());
                assertTrue(result.result() == 2);

                log.lastIndex(new Handler<AsyncResult<Long>>() {
                  @Override
                  public void handle(AsyncResult<Long> result) {
                    assertTrue(result.succeeded());
                    assertTrue(result.result() == 2);
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
  public void testLastEntry() {
    final Log log = new MemoryLog();
    log.appendEntry(new NoOpEntry(), new Handler<AsyncResult<Long>>() {
      @Override
      public void handle(AsyncResult<Long> result) {
        assertTrue(result.succeeded());
        assertTrue(result.result() == 0);

        log.appendEntry(new ConfigurationEntry(), new Handler<AsyncResult<Long>>() {
          @Override
          public void handle(AsyncResult<Long> result) {
            assertTrue(result.succeeded());
            assertTrue(result.result() == 1);

            log.appendEntry(new CommandEntry(1, new DefaultCommand<Object>()), new Handler<AsyncResult<Long>>() {
              @Override
              public void handle(AsyncResult<Long> result) {
                assertTrue(result.succeeded());
                assertTrue(result.result() == 2);

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
