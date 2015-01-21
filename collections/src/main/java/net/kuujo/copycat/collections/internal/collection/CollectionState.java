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
package net.kuujo.copycat.collections.internal.collection;

import net.kuujo.copycat.state.Command;
import net.kuujo.copycat.state.Initializer;
import net.kuujo.copycat.state.Query;
import net.kuujo.copycat.state.StateContext;
import net.kuujo.copycat.protocol.Consistency;

import java.util.Collection;

/**
 * Asynchronous collection state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface CollectionState<T extends CollectionState<T, U>, U> extends Collection<U> {

  /**
   * Initializes the state.
   *
   * @param context The state context.
   */
  @Initializer
  void init(StateContext<T> context);

  @Override
  @Command
  boolean add(U value);

  @Override
  @Command
  boolean addAll(Collection<? extends U> c);

  @Override
  @Command
  boolean retainAll(Collection<?> c);

  @Override
  @Command
  boolean remove(Object value);

  @Override
  @Command
  boolean removeAll(Collection<?> c);

  @Override
  @Query(consistency=Consistency.DEFAULT)
  boolean contains(Object value);

  @Override
  @Query(consistency=Consistency.DEFAULT)
  boolean containsAll(Collection<?> c);

  @Override
  @Query(consistency=Consistency.DEFAULT)
  int size();

  @Override
  @Query(consistency=Consistency.DEFAULT)
  boolean isEmpty();

  @Override
  @Command
  void clear();

}
