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

import net.kuujo.copycat.Replica;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.annotations.Command;
import net.kuujo.copycat.impl.DefaultReplica;
import net.kuujo.copycat.log.CommandEntry;
import net.kuujo.copycat.log.ConfigurationEntry;
import net.kuujo.copycat.log.Entry;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;

import static org.vertx.testtools.VertxAssert.assertTrue;
import static org.vertx.testtools.VertxAssert.assertFalse;
import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.testComplete;

import org.vertx.testtools.TestVerticle;

/**
 * A replication test.
 *
 * @author Jordan Halterman
 */
public class ReplicationTest extends TestVerticle {

  public static class TestStateMachine implements StateMachine {
    @SuppressWarnings("unused")
    private String data;

    @Command(name="test", type=Command.Type.WRITE)
    public boolean test(@Command.Argument("data") String data) {
      this.data = data;
      return true;
    }
  }

  @Test
  public void testNoOpReplication() {
    final Replica test1 = new DefaultReplica("test.1", vertx, new TestStateMachine());
    test1.config().setMembers("test.1", "test.2", "test.3");
    test1.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());

        final Replica test2 = new DefaultReplica("test.2", vertx, new TestStateMachine());
        test2.config().setMembers("test.1", "test.2", "test.3");
        test2.start(new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());

            final Replica test3 = new DefaultReplica("test.3", vertx, new TestStateMachine());
            test3.config().setMembers("test.1", "test.2", "test.3");
            test3.start(new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                assertTrue(result.succeeded());
                test3.submitCommand("test", new JsonObject().putString("data", "Hello world!"), new Handler<AsyncResult<Boolean>>() {
                  @Override
                  public void handle(AsyncResult<Boolean> result) {
                    assertTrue(result.succeeded());
                    test1.log().entry(1, new Handler<AsyncResult<Entry>>() {
                      @Override
                      public void handle(AsyncResult<Entry> result) {
                        assertTrue(result.succeeded());
                        assertEquals(Entry.Type.NOOP, result.result().type());
                      }
                    });
                    test2.log().entry(1, new Handler<AsyncResult<Entry>>() {
                      @Override
                      public void handle(AsyncResult<Entry> result) {
                        assertTrue(result.succeeded());
                        assertEquals(Entry.Type.NOOP, result.result().type());
                      }
                    });
                    test3.log().entry(1, new Handler<AsyncResult<Entry>>() {
                      @Override
                      public void handle(AsyncResult<Entry> result) {
                        assertTrue(result.succeeded());
                        assertEquals(Entry.Type.NOOP, result.result().type());
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
    });
  }

  @Test
  public void testCommandReplication() {
    final Replica test1 = new DefaultReplica("test.1", vertx, new TestStateMachine());
    test1.config().setMembers("test.1", "test.2", "test.3");
    test1.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());

        final Replica test2 = new DefaultReplica("test.2", vertx, new TestStateMachine());
        test2.config().setMembers("test.1", "test.2", "test.3");
        test2.start(new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());

            final Replica test3 = new DefaultReplica("test.3", vertx, new TestStateMachine());
            test3.config().setMembers("test.1", "test.2", "test.3");
            test3.start(new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                assertTrue(result.succeeded());
                test3.submitCommand("test", new JsonObject().putString("data", "Hello world!"), new Handler<AsyncResult<Void>>() {
                  @Override
                  public void handle(AsyncResult<Void> result) {
                    assertTrue(result.succeeded());
                    // We have to set a timer after submitting the command because
                    // the command may not necessarily be replicated to all of the
                    // nodes. It is only required that the command entry be immediately
                    // replicated to a majority of the nodes.
                    vertx.setTimer(1000, new Handler<Long>() {
                      @Override
                      public void handle(Long timerID) {
                        test1.log().entry(4, new Handler<AsyncResult<Entry>>() {
                          @Override
                          public void handle(AsyncResult<Entry> result) {
                            assertTrue(result.succeeded());
                            assertEquals(Entry.Type.COMMAND, result.result().type());
                            assertEquals("test", ((CommandEntry) result.result()).command());
                            assertEquals("Hello world!", ((CommandEntry) result.result()).args().getString("data"));
                          }
                        });
                        test2.log().entry(4, new Handler<AsyncResult<Entry>>() {
                          @Override
                          public void handle(AsyncResult<Entry> result) {
                            assertTrue(result.succeeded());
                            assertEquals(Entry.Type.COMMAND, result.result().type());
                            assertEquals("test", ((CommandEntry) result.result()).command());
                            assertEquals("Hello world!", ((CommandEntry) result.result()).args().getString("data"));
                          }
                        });
                        test3.log().entry(4, new Handler<AsyncResult<Entry>>() {
                          @Override
                          public void handle(AsyncResult<Entry> result) {
                            assertTrue(result.succeeded());
                            assertEquals(Entry.Type.COMMAND, result.result().type());
                            assertEquals("test", ((CommandEntry) result.result()).command());
                            assertEquals("Hello world!", ((CommandEntry) result.result()).args().getString("data"));
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
        });
      }
    });
  }

  @Test
  public void testConfigReplication() {
    // Start the three replicas indicated by the configuration.
    final Replica test1 = new DefaultReplica("test.1", vertx, new TestStateMachine()).setHeartbeatInterval(100);
    test1.config().setMembers("test.1", "test.2", "test.3");
    test1.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());

        final Replica test2 = new DefaultReplica("test.2", vertx, new TestStateMachine()).setHeartbeatInterval(100);
        test2.config().setMembers("test.1", "test.2", "test.3");
        test2.start(new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());

            final Replica test3 = new DefaultReplica("test.3", vertx, new TestStateMachine()).setHeartbeatInterval(100);
            test3.config().setMembers("test.1", "test.2", "test.3");
            test3.start(new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                assertTrue(result.succeeded());
                vertx.setTimer(500, new Handler<Long>() {
                  @Override
                  public void handle(Long timerID) {
                     // When the replicas are started and a leader is elected, the leader
                    // should commit a configuration entry to the log. This will contain
                    // the initial cluster configuration. Check the logs for that. There
                    // will be two entries representing the "old" configuration and the new.
                    test1.log().entry(2, new Handler<AsyncResult<Entry>>() {
                      @Override
                      public void handle(AsyncResult<Entry> result) {
                        assertTrue(result.succeeded());
                        assertEquals(Entry.Type.CONFIGURATION, result.result().type());
                        assertTrue(((ConfigurationEntry) result.result()).members().contains("test.1"));
                        assertTrue(((ConfigurationEntry) result.result()).members().contains("test.2"));
                        assertTrue(((ConfigurationEntry) result.result()).members().contains("test.3"));
                        assertFalse(((ConfigurationEntry) result.result()).members().contains("test.4"));
                      }
                    });
                    test2.log().entry(2, new Handler<AsyncResult<Entry>>() {
                      @Override
                      public void handle(AsyncResult<Entry> result) {
                        assertTrue(result.succeeded());
                        assertEquals(Entry.Type.CONFIGURATION, result.result().type());
                        assertTrue(((ConfigurationEntry) result.result()).members().contains("test.1"));
                        assertTrue(((ConfigurationEntry) result.result()).members().contains("test.2"));
                        assertTrue(((ConfigurationEntry) result.result()).members().contains("test.3"));
                        assertFalse(((ConfigurationEntry) result.result()).members().contains("test.4"));
                      }
                    });
                    test3.log().entry(2, new Handler<AsyncResult<Entry>>() {
                      @Override
                      public void handle(AsyncResult<Entry> result) {
                        assertTrue(result.succeeded());
                        assertEquals(Entry.Type.CONFIGURATION, result.result().type());
                        assertTrue(((ConfigurationEntry) result.result()).members().contains("test.1"));
                        assertTrue(((ConfigurationEntry) result.result()).members().contains("test.2"));
                        assertTrue(((ConfigurationEntry) result.result()).members().contains("test.3"));
                        assertFalse(((ConfigurationEntry) result.result()).members().contains("test.4"));
                      }
                    });
                    test1.log().entry(3, new Handler<AsyncResult<Entry>>() {
                      @Override
                      public void handle(AsyncResult<Entry> result) {
                        assertTrue(result.succeeded());
                        assertEquals(Entry.Type.CONFIGURATION, result.result().type());
                        assertTrue(((ConfigurationEntry) result.result()).members().contains("test.1"));
                        assertTrue(((ConfigurationEntry) result.result()).members().contains("test.2"));
                        assertTrue(((ConfigurationEntry) result.result()).members().contains("test.3"));
                        assertFalse(((ConfigurationEntry) result.result()).members().contains("test.4"));
                      }
                    });
                    test2.log().entry(3, new Handler<AsyncResult<Entry>>() {
                      @Override
                      public void handle(AsyncResult<Entry> result) {
                        assertTrue(result.succeeded());
                        assertEquals(Entry.Type.CONFIGURATION, result.result().type());
                        assertTrue(((ConfigurationEntry) result.result()).members().contains("test.1"));
                        assertTrue(((ConfigurationEntry) result.result()).members().contains("test.2"));
                        assertTrue(((ConfigurationEntry) result.result()).members().contains("test.3"));
                        assertFalse(((ConfigurationEntry) result.result()).members().contains("test.4"));
                      }
                    });
                    test3.log().entry(3, new Handler<AsyncResult<Entry>>() {
                      @Override
                      public void handle(AsyncResult<Entry> result) {
                        assertTrue(result.succeeded());
                        assertEquals(Entry.Type.CONFIGURATION, result.result().type());
                        assertTrue(((ConfigurationEntry) result.result()).members().contains("test.1"));
                        assertTrue(((ConfigurationEntry) result.result()).members().contains("test.2"));
                        assertTrue(((ConfigurationEntry) result.result()).members().contains("test.3"));
                        assertFalse(((ConfigurationEntry) result.result()).members().contains("test.4"));
                      }
                    });

                    // Now, try adding a member to the cluster.
                    test1.config().addMember("test.4");
                    test2.config().addMember("test.4");
                    test3.config().addMember("test.4");

                    // Submit a command to the cluster for good measure.
                    test3.submitCommand("test", new JsonObject().putString("data", "Hello world!"), new Handler<AsyncResult<Void>>() {
                      @Override
                      public void handle(AsyncResult<Void> result) {
                        // Wait for the logs to be replicated.
                        vertx.setTimer(1000, new Handler<Long>() {
                          @Override
                          public void handle(Long event) {
                            // Once the logs have been replicates, the leader should have created
                            // and replicated two more configuration entries. The first will be
                            // a combined configuration and the second will be the new configuration.
                            // In cases where nodes are being added, the entries will essentially
                            // be the new configuration, but in cases where nodes are removed,
                            // the entries will essentially be the old configuration and the new
                            // configuration separately.
                            // In this case we're adding a node. Check both entries for the new
                            // cluster membership set.
                            test1.log().entry(4, new Handler<AsyncResult<Entry>>() {
                              @Override
                              public void handle(AsyncResult<Entry> result) {
                                assertTrue(result.succeeded());
                                assertEquals(Entry.Type.CONFIGURATION, result.result().type());
                                assertTrue(((ConfigurationEntry) result.result()).members().contains("test.1"));
                                assertTrue(((ConfigurationEntry) result.result()).members().contains("test.2"));
                                assertTrue(((ConfigurationEntry) result.result()).members().contains("test.3"));
                                assertTrue(((ConfigurationEntry) result.result()).members().contains("test.4"));
                              }
                            });
                            test1.log().entry(4, new Handler<AsyncResult<Entry>>() {
                              @Override
                              public void handle(AsyncResult<Entry> result) {
                                assertTrue(result.succeeded());
                                assertEquals(Entry.Type.CONFIGURATION, result.result().type());
                                assertTrue(((ConfigurationEntry) result.result()).members().contains("test.1"));
                                assertTrue(((ConfigurationEntry) result.result()).members().contains("test.2"));
                                assertTrue(((ConfigurationEntry) result.result()).members().contains("test.3"));
                                assertTrue(((ConfigurationEntry) result.result()).members().contains("test.4"));
                              }
                            });
                            test2.log().entry(6, new Handler<AsyncResult<Entry>>() {
                              @Override
                              public void handle(AsyncResult<Entry> result) {
                                assertTrue(result.succeeded());
                                assertEquals(Entry.Type.CONFIGURATION, result.result().type());
                                assertTrue(((ConfigurationEntry) result.result()).members().contains("test.1"));
                                assertTrue(((ConfigurationEntry) result.result()).members().contains("test.2"));
                                assertTrue(((ConfigurationEntry) result.result()).members().contains("test.3"));
                                assertTrue(((ConfigurationEntry) result.result()).members().contains("test.4"));
                              }
                            });
                            test2.log().entry(6, new Handler<AsyncResult<Entry>>() {
                              @Override
                              public void handle(AsyncResult<Entry> result) {
                                assertTrue(result.succeeded());
                                assertEquals(Entry.Type.CONFIGURATION, result.result().type());
                                assertTrue(((ConfigurationEntry) result.result()).members().contains("test.1"));
                                assertTrue(((ConfigurationEntry) result.result()).members().contains("test.2"));
                                assertTrue(((ConfigurationEntry) result.result()).members().contains("test.3"));
                                assertTrue(((ConfigurationEntry) result.result()).members().contains("test.4"));
                              }
                            });
                            test3.log().entry(6, new Handler<AsyncResult<Entry>>() {
                              @Override
                              public void handle(AsyncResult<Entry> result) {
                                assertTrue(result.succeeded());
                                assertEquals(Entry.Type.CONFIGURATION, result.result().type());
                                assertTrue(((ConfigurationEntry) result.result()).members().contains("test.1"));
                                assertTrue(((ConfigurationEntry) result.result()).members().contains("test.2"));
                                assertTrue(((ConfigurationEntry) result.result()).members().contains("test.3"));
                                assertTrue(((ConfigurationEntry) result.result()).members().contains("test.4"));
                              }
                            });

                            // Now try removing a member from the cluster. Again, this should create
                            // two new configuration entries, one with the combined configuration and
                            // one with the new configuration.
                            test1.config().removeMember("test.4");
                            test2.config().removeMember("test.4");
                            test3.config().removeMember("test.4");

                            vertx.setTimer(1000, new Handler<Long>() {
                              @Override
                              public void handle(Long event) {
                                test1.log().entry(7, new Handler<AsyncResult<Entry>>() {
                                  @Override
                                  public void handle(AsyncResult<Entry> result) {
                                    assertTrue(result.succeeded());
                                    assertEquals(Entry.Type.CONFIGURATION, result.result().type());
                                    assertTrue(((ConfigurationEntry) result.result()).members().contains("test.1"));
                                    assertTrue(((ConfigurationEntry) result.result()).members().contains("test.2"));
                                    assertTrue(((ConfigurationEntry) result.result()).members().contains("test.3"));
                                    assertTrue(((ConfigurationEntry) result.result()).members().contains("test.4"));
                                  }
                                });
                                test2.log().entry(7, new Handler<AsyncResult<Entry>>() {
                                  @Override
                                  public void handle(AsyncResult<Entry> result) {
                                    assertTrue(result.succeeded());
                                    assertEquals(Entry.Type.CONFIGURATION, result.result().type());
                                    assertTrue(((ConfigurationEntry) result.result()).members().contains("test.1"));
                                    assertTrue(((ConfigurationEntry) result.result()).members().contains("test.2"));
                                    assertTrue(((ConfigurationEntry) result.result()).members().contains("test.3"));
                                    assertTrue(((ConfigurationEntry) result.result()).members().contains("test.4"));
                                  }
                                });
                                test3.log().entry(7, new Handler<AsyncResult<Entry>>() {
                                  @Override
                                  public void handle(AsyncResult<Entry> result) {
                                    assertTrue(result.succeeded());
                                    assertEquals(Entry.Type.CONFIGURATION, result.result().type());
                                    assertTrue(((ConfigurationEntry) result.result()).members().contains("test.1"));
                                    assertTrue(((ConfigurationEntry) result.result()).members().contains("test.2"));
                                    assertTrue(((ConfigurationEntry) result.result()).members().contains("test.3"));
                                    assertTrue(((ConfigurationEntry) result.result()).members().contains("test.4"));
                                  }
                                });
                                test1.log().entry(8, new Handler<AsyncResult<Entry>>() {
                                  @Override
                                  public void handle(AsyncResult<Entry> result) {
                                    assertTrue(result.succeeded());
                                    assertEquals(Entry.Type.CONFIGURATION, result.result().type());
                                    assertTrue(((ConfigurationEntry) result.result()).members().contains("test.1"));
                                    assertTrue(((ConfigurationEntry) result.result()).members().contains("test.2"));
                                    assertTrue(((ConfigurationEntry) result.result()).members().contains("test.3"));
                                    assertFalse(((ConfigurationEntry) result.result()).members().contains("test.4"));
                                  }
                                });
                                test2.log().entry(8, new Handler<AsyncResult<Entry>>() {
                                  @Override
                                  public void handle(AsyncResult<Entry> result) {
                                    assertTrue(result.succeeded());
                                    assertEquals(Entry.Type.CONFIGURATION, result.result().type());
                                    assertTrue(((ConfigurationEntry) result.result()).members().contains("test.1"));
                                    assertTrue(((ConfigurationEntry) result.result()).members().contains("test.2"));
                                    assertTrue(((ConfigurationEntry) result.result()).members().contains("test.3"));
                                    assertFalse(((ConfigurationEntry) result.result()).members().contains("test.4"));
                                  }
                                });
                                test3.log().entry(8, new Handler<AsyncResult<Entry>>() {
                                  @Override
                                  public void handle(AsyncResult<Entry> result) {
                                    assertTrue(result.succeeded());
                                    assertEquals(Entry.Type.CONFIGURATION, result.result().type());
                                    assertTrue(((ConfigurationEntry) result.result()).members().contains("test.1"));
                                    assertTrue(((ConfigurationEntry) result.result()).members().contains("test.2"));
                                    assertTrue(((ConfigurationEntry) result.result()).members().contains("test.3"));
                                    assertFalse(((ConfigurationEntry) result.result()).members().contains("test.4"));
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
                });
              }
            });
          }
        });
      }
    });
  }

}
