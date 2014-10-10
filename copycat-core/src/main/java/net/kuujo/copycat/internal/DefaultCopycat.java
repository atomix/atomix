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
import net.kuujo.copycat.spi.service.Service;

/**
 * Primary copycat API.<p>
 *
 * The <code>CopyCat</code> class provides a fluent API for
 * combining the {@link DefaultCopycatContext} with an {@link net.kuujo.copycat.spi.service.Service}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultCopycat extends AbstractCopycat<CopycatContext> implements Copycat {
  private final Service service;

  DefaultCopycat(Service service, CopycatContext context) {
    super(context);
    this.service = service;
    this.service.init(context);
  }

  @Override
  public void start() {
    context.start();
    service.start();
  }

  @Override
  public void stop() {
    service.stop();
    context.stop();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
