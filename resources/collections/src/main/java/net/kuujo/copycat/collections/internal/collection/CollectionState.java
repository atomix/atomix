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

import net.kuujo.copycat.raft.Consistency;
import net.kuujo.copycat.state.Read;
import net.kuujo.copycat.state.Write;

import java.util.Collection;

/**
 * Asynchronous collection status.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface CollectionState<T extends CollectionState<T, U>, U> extends Collection<U> {

  @Override
  @Write
  boolean add(U value);

  @Override
  @Write
  boolean addAll(Collection<? extends U> c);

  @Override
  @Write
  boolean retainAll(Collection<?> c);

  @Override
  @Write
  boolean remove(Object value);

  @Override
  @Write
  boolean removeAll(Collection<?> c);

  @Override
  @Read(consistency=Consistency.DEFAULT)
  boolean contains(Object value);

  @Override
  @Read(consistency=Consistency.DEFAULT)
  boolean containsAll(Collection<?> c);

  @Override
  @Read(consistency=Consistency.DEFAULT)
  int size();

  @Override
  @Read(consistency=Consistency.DEFAULT)
  boolean isEmpty();

  @Override
  @Write
  void clear();

}
