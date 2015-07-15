/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.util;

import io.netty.channel.EventLoop;
import net.kuujo.alleycat.Alleycat;
import net.kuujo.copycat.util.concurrent.Context;
import net.kuujo.copycat.util.concurrent.CopycatThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Netty context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NettyContext extends Context {
  private static final Logger LOGGER = LoggerFactory.getLogger(NettyContext.class);
  private final EventLoop eventLoop;

  public NettyContext(String name, EventLoop eventLoop, Alleycat serializer) {
    super(serializer);
    Thread thread = getThread(eventLoop);
    thread.setName(name);
    this.eventLoop = eventLoop;
  }

  private static CopycatThread getThread(EventLoop eventLoop) {
    final AtomicReference<CopycatThread> thread = new AtomicReference<>();
    try {
      eventLoop.submit(() -> {
        thread.set((CopycatThread) Thread.currentThread());
      }).get();
      eventLoop.submit(new Runnable() {
        @Override
        public void run() {
          thread.set((CopycatThread) Thread.currentThread());
        }
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException("failed to initialize thread state", e);
    }
    return thread.get();
  }

  @Override
  public void execute(Runnable command) {
    eventLoop.execute(command);
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable runnable, long delay, TimeUnit unit) {
    return eventLoop.schedule(wrapRunnable(runnable), delay, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable runnable, long delay, long rate, TimeUnit unit) {
    return eventLoop.scheduleAtFixedRate(wrapRunnable(runnable), delay, rate, unit);
  }

  /**
   * Wraps a runnable in an uncaught exception handler.
   */
  private Runnable wrapRunnable(final Runnable runnable) {
    return new Runnable() {
      @Override
      public void run() {
        try {
          runnable.run();
        } catch (RuntimeException e) {
          LOGGER.error("An uncaught exception occurred: {}", e);
          e.printStackTrace();
        }
      }
    };
  }

  @Override
  public void close() {
    eventLoop.shutdownGracefully();
  }

}
