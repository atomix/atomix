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
package net.kuujo.copycat.atomic.internal;

import net.kuujo.copycat.raft.Consistency;
import net.kuujo.copycat.state.Read;
import net.kuujo.copycat.state.Write;

/**
 * Atomic long status.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface LongState {

  @Read(consistency= Consistency.STRONG)
  long get();

  @Write
  void set(long value);

  @Write
  long addAndGet(long value);

  @Write
  long getAndAdd(long value);

  @Write
  long getAndSet(long value);

  @Write
  long getAndIncrement();

  @Write
  long getAndDecrement();

  @Write
  long incrementAndGet();

  @Write
  long decrementAndGet();

  @Write
  boolean compareAndSet(long expect, long update);

}
