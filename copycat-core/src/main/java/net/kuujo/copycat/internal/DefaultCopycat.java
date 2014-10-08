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
package net.kuujo.copycat.internal;

import net.kuujo.copycat.Copycat;
import net.kuujo.copycat.CopycatContext;
import net.kuujo.copycat.event.*;
import net.kuujo.copycat.internal.util.Args;
import net.kuujo.copycat.spi.service.CopycatService;

import java.util.concurrent.CompletableFuture;

/**
 * Primary copycat API.<p>
 *
 * The <code>CopyCat</code> class provides a fluent API for
 * combining the {@link DefaultCopycatContext} with an {@link net.kuujo.copycat.spi.service.CopycatService}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultCopycat implements Copycat {
  private final CopycatService service;
  private final CopycatContext context;

  DefaultCopycat(CopycatService service, CopycatContext context) {
    this.service = service;
    this.context = context;
    this.service.init(context);
  }

  @Override
  public CopycatContext context() {
    return context;
  }

  @Override
  public Events on() {
    return context.on();
  }

  @Override
  public <T extends Event> EventContext<T> on(Class<T> event) {
    return context.on().event(event);
  }

  @Override
  public EventHandlers events() {
    return context.events();
  }

  @Override
  public <T extends Event> EventHandlerRegistry<T> event(Class<T> event) {
    return context.event(event);
  }

  @Override
  public CompletableFuture<Void> start() {
    return context.start().thenRun(()->{});
  }

  @Override
  public CompletableFuture<Void> stop() {
    return service.stop();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
