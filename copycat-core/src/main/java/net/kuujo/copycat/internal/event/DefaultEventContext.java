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
package net.kuujo.copycat.internal.event;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import net.kuujo.copycat.event.Event;
import net.kuujo.copycat.event.EventContext;
import net.kuujo.copycat.event.EventHandler;
import net.kuujo.copycat.event.EventHandlerRegistry;

/**
 * Default event context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 *
 * @param <E> The event type.
 */
public class DefaultEventContext<E extends Event> implements EventContext<E>, EventHandler<E> {
  private final Executor executor = Executors.newCachedThreadPool();
  private HandlerHolder handler;

  private class HandlerHolder {
    private final EventHandler<E> handler;
    private final boolean once;
    private final boolean async;

    private HandlerHolder(EventHandler<E> handler, boolean once, boolean async) {
      this.handler = handler;
      this.once = once;
      this.async = async;
    }

    public void run(E event) {
      if (async) {
        executor.execute(() -> handler.handle(event));
      } else {
        handler.handle(event);
      }
      if (once) {
        DefaultEventContext.this.handler = null;
      }
    }

  }

  public DefaultEventContext(EventHandlerRegistry<E> registry) {
    registry.registerHandler(this);
  }

  @Override
  public void handle(E event) {
    if (handler != null) {
      handler.run(event);
    }
  }

  @Override
  public EventContext<E> run(EventHandler<E> handler) {
    this.handler = new HandlerHolder(handler, false, false);
    return this;
  }

  @Override
  public EventContext<E> runAsync(EventHandler<E> handler) {
    this.handler = new HandlerHolder(handler, false, true);
    return this;
  }

  @Override
  public EventContext<E> runOnce(EventHandler<E> handler) {
    this.handler = new HandlerHolder(handler, true, false);
    return this;
  }

  @Override
  public EventContext<E> runOnceAsync(EventHandler<E> handler) {
    this.handler = new HandlerHolder(handler, true, true);
    return this;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
