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

import net.kuujo.copycat.Command;
import net.kuujo.copycat.Query;
import net.kuujo.copycat.protocol.Consistency;

import java.util.Collection;
import java.util.List;

/**
 * Asynchronous list state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ListState<T> extends CollectionState<ListState<T>, T>, List<T> {

  @Override
  @Query(consistency=Consistency.DEFAULT)
  T get(int index);

  @Override
  @Command
  T set(int index, T element);

  @Override
  @Command
  void add(int index, T element);

  @Override
  @Command
  boolean addAll(int index, Collection<? extends T> c);

  @Override
  @Command
  T remove(int index);

}
