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
 * Atomic reference status.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ReferenceState<T> {

  @Read(consistency= Consistency.STRONG)
  T get();

  @Write
  void set(T value);

  @Write
  T getAndSet(T value);

  @Write
  boolean compareAndSet(T expect, T update);

}
