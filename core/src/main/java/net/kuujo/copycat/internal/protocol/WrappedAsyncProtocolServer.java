/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.internal.protocol;

import net.kuujo.copycat.internal.util.concurrent.NamedThreadFactory;
import net.kuujo.copycat.protocol.*;
import net.kuujo.copycat.spi.protocol.AsyncProtocolServer;
import net.kuujo.copycat.spi.protocol.ProtocolServer;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Asynchronous protocol server wrapper.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class WrappedAsyncProtocolServer implements AsyncProtocolServer {
  private static final ThreadFactory THREAD_FACTORY = new NamedThreadFactory("wrapped-async-protocol-server-%s");
  private final Executor executor = Executors.newCachedThreadPool(THREAD_FACTORY);
  private final ProtocolServer server;

  public WrappedAsyncProtocolServer(ProtocolServer server) {
    this.server = server;
  }

  @Override
  public void requestHandler(final AsyncRequestHandler handler) {
    server.requestHandler(new RequestHandler() {
      private void await(CountDownLatch latch) {
        try {
          latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          throw new ProtocolException(e);
        }
      }
      @Override
      public PingResponse ping(PingRequest request) {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<PingResponse> result = new AtomicReference<>();
        handler.ping(request).thenAccept(result::set);
        await(latch);
        return result.get();
      }
      @Override
      public SyncResponse sync(SyncRequest request) {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<SyncResponse> result = new AtomicReference<>();
        handler.sync(request).thenAccept(result::set);
        await(latch);
        return result.get();
      }
      @Override
      public PollResponse poll(PollRequest request) {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<PollResponse> result = new AtomicReference<>();
        handler.poll(request).thenAccept(result::set);
        await(latch);
        return result.get();
      }
      @Override
      public SubmitResponse submit(SubmitRequest request) {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<SubmitResponse> result = new AtomicReference<>();
        handler.submit(request).thenAccept(result::set);
        await(latch);
        return result.get();
      }
    });
  }

  @Override
  public CompletableFuture<Void> listen() {
    return CompletableFuture.runAsync(server::listen, executor);
  }

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.runAsync(server::close, executor);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
