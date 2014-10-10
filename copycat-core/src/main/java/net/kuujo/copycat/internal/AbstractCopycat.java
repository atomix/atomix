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
package net.kuujo.copycat.internal;

import net.kuujo.copycat.BaseCopycat;
import net.kuujo.copycat.BaseCopycatContext;
import net.kuujo.copycat.event.*;

/**
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class AbstractCopycat<T extends BaseCopycatContext> implements BaseCopycat<T> {
  protected final T context;

  protected AbstractCopycat(T context) {
    this.context = context;
  }

  @Override
  public T context() {
    return context;
  }

  @Override
  public Events on() {
    return context.on();
  }

  @Override
  public <T1 extends Event> EventContext<T1> on(Class<T1> event) {
    return context.on(event);
  }

  @Override
  public EventHandlers events() {
    return context.events();
  }

  @Override
  public <T1 extends Event> EventHandlerRegistry<T1> event(Class<T1> event) {
    return context.event(event);
  }

}
