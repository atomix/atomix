package net.kuujo.copycat;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.assertTrue;
import static org.vertx.testtools.VertxAssert.fail;
import static org.vertx.testtools.VertxAssert.testComplete;

import java.util.ArrayList;

import net.kuujo.copycat.StateClient;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.PingResponse;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.PollResponse;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.protocol.SubmitResponse;
import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.protocol.SyncResponse;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

public class StateClientTest extends TestVerticle {

  @Test
  public void testPingRequestResponse() {
    final StateClient client1 = new StateClient("foo", vertx);
    client1.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          fail(result.cause().getMessage());
        } else {
          final StateClient client2 = new StateClient("bar", vertx);
          client2.start(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (result.failed()) {
                fail(result.cause().getMessage());
              } else {
                client2.pingHandler(new Handler<PingRequest>() {
                  @Override
                  public void handle(PingRequest request) {
                    assertEquals(1, request.term());
                    assertEquals("foo", request.leader());
                    request.reply(1);
                  }
                });
                client1.ping("bar", new PingRequest(1, "foo"), new Handler<AsyncResult<PingResponse>>() {
                  @Override
                  public void handle(AsyncResult<PingResponse> result) {
                    if (result.failed()) {
                      fail(result.cause().getMessage());
                    } else {
                      assertEquals(1, result.result().term());
                      testComplete();
                    }
                  }
                });
              }
            }
          });
        }
      }
    });
  }

  @Test
  public void testPollRequestResponse() {
    final StateClient client1 = new StateClient("foo", vertx);
    client1.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          fail(result.cause().getMessage());
        } else {
          final StateClient client2 = new StateClient("bar", vertx);
          client2.start(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (result.failed()) {
                fail(result.cause().getMessage());
              } else {
                client2.pollHandler(new Handler<PollRequest>() {
                  @Override
                  public void handle(PollRequest request) {
                    assertEquals(1, request.term());
                    assertEquals("foo", request.candidate());
                    assertEquals(0, request.lastLogIndex());
                    assertEquals(0, request.lastLogTerm());
                    request.reply(1, true);
                  }
                });
                client1.poll("bar", new PollRequest(1, "foo", 0, 0), new Handler<AsyncResult<PollResponse>>() {
                  @Override
                  public void handle(AsyncResult<PollResponse> result) {
                    if (result.failed()) {
                      fail(result.cause().getMessage());
                    } else {
                      assertEquals(1, result.result().term());
                      assertTrue(result.result().voteGranted());
                      testComplete();
                    }
                  }
                });
              }
            }
          });
        }
      }
    });
  }

  @Test
  public void testSyncRequestResponse() {
    final StateClient client1 = new StateClient("foo", vertx);
    client1.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          fail(result.cause().getMessage());
        } else {
          final StateClient client2 = new StateClient("bar", vertx);
          client2.start(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (result.failed()) {
                fail(result.cause().getMessage());
              } else {
                client2.syncHandler(new Handler<SyncRequest>() {
                  @Override
                  public void handle(SyncRequest request) {
                    assertEquals(10, request.term());
                    assertEquals("foo", request.leader());
                    assertEquals(4, request.commit());
                    assertEquals(8, request.prevLogIndex());
                    assertEquals(6, request.prevLogTerm());
                    assertEquals(0, request.entries().size());
                    request.reply(1, true);
                  }
                });
                client1.sync("bar", new SyncRequest(10, "foo", 8, 6, new ArrayList<Entry>(), 4), new Handler<AsyncResult<SyncResponse>>() {
                  @Override
                  public void handle(AsyncResult<SyncResponse> result) {
                    if (result.failed()) {
                      fail(result.cause().getMessage());
                    } else {
                      assertEquals(1, result.result().term());
                      assertTrue(result.result().success());
                      testComplete();
                    }
                  }
                });
              }
            }
          });
        }
      }
    });
  }

  @Test
  public void testSubmitRequestResponse() {
    final StateClient client1 = new StateClient("foo", vertx);
    client1.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          fail(result.cause().getMessage());
        } else {
          final StateClient client2 = new StateClient("bar", vertx);
          client2.start(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (result.failed()) {
                fail(result.cause().getMessage());
              } else {
                client2.submitHandler(new Handler<SubmitRequest>() {
                  @Override
                  public void handle(SubmitRequest request) {
                    assertEquals("foobarbaz", request.command());
                    assertEquals("bar", request.args().getString("foo"));
                    request.reply(new JsonObject().putString("foo", "bar"));
                  }
                });
                client1.submit("bar", new SubmitRequest("foobarbaz", new JsonObject().putString("foo", "bar")), new Handler<AsyncResult<SubmitResponse>>() {
                  @Override
                  public void handle(AsyncResult<SubmitResponse> result) {
                    if (result.failed()) {
                      fail(result.cause().getMessage());
                    } else {
                      assertEquals("bar", result.result().result().getString("foo"));
                      testComplete();
                    }
                  }
                });
              }
            }
          });
        }
      }
    });
  }

}
