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
package net.kuujo.copycat.internal.util.concurrent;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Named thread factory.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NamedThreadFactory implements ThreadFactory {
  private final AtomicInteger threadNumber = new AtomicInteger(1);
  private final String nameFormat;

  /**
   * Creates a thread factory that names threads according to the {@code nameFormat} by supplying a
   * single argument to the format representing the thread number.
   */
  public NamedThreadFactory(String nameFormat) {
    this.nameFormat = nameFormat;
  }

  @Override
  public Thread newThread(Runnable r) {
    return new Thread(r, String.format(nameFormat, threadNumber.getAndIncrement()));
  }
}
