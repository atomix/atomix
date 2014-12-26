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

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;

/**
 * Default asynchronous list state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultListState<T> extends AbstractCollectionState<ListState<T>, List<T>, T> implements ListState<T> {

  @Override
  protected List<T> createCollection() {
    return new ArrayList<>();
  }

  @Override
  public T get(int index) {
    return collection.get(index);
  }

  @Override
  public T set(int index, T value) {
    return collection.set(index, value);
  }

  @Override
  public boolean addAll(int index, Collection<? extends T> c) {
    return collection.addAll(index, c);
  }

  @Override
  public void add(int index, T element) {
    collection.add(index, element);
  }

  @Override
  public T remove(int index) {
    return collection.remove(index);
  }

  @Override
  public int indexOf(Object o) {
    return collection.indexOf(o);
  }

  @Override
  public int lastIndexOf(Object o) {
    return collection.lastIndexOf(o);
  }

  @NotNull
  @Override
  public ListIterator<T> listIterator() {
    throw new UnsupportedOperationException("listIterator");
  }

  @NotNull
  @Override
  public ListIterator<T> listIterator(int index) {
    throw new UnsupportedOperationException("listIterator");
  }

  @NotNull
  @Override
  public List<T> subList(int fromIndex, int toIndex) {
    return collection.subList(fromIndex, toIndex);
  }

}
