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

import net.kuujo.copycat.Initializer;
import net.kuujo.copycat.StateContext;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Iterator;

/**
 * Abstract asynchronous collection state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractCollectionState<T extends CollectionState<T, V>, U extends Collection<V>, V> implements CollectionState<T, V> {
  protected U collection;

  /**
   * Creates the collection.
   */
  protected abstract U createCollection();

  @Override
  @Initializer
  public void init(StateContext<T> context) {
    collection = context.get("value");
    if (collection == null) {
      collection = createCollection();
      context.put("value", collection);
    }
  }

  @Override
  public boolean add(V value) {
    return collection.add(value);
  }

  @Override
  public boolean addAll(Collection<? extends V> c) {
    return collection.addAll(c);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return collection.retainAll(c);
  }

  @Override
  public boolean remove(Object value) {
    return collection.remove(value);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return collection.removeAll(c);
  }

  @Override
  public boolean contains(Object value) {
    return collection.contains(value);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return collection.containsAll(c);
  }

  @Override
  public int size() {
    return collection.size();
  }

  @Override
  public boolean isEmpty() {
    return collection.isEmpty();
  }

  @Override
  public void clear() {
    collection.clear();
  }

  @NotNull
  @Override
  public Iterator<V> iterator() {
    throw new UnsupportedOperationException("Cannot iterate collection state");
  }

  @NotNull
  @Override
  public Object[] toArray() {
    return collection.toArray();
  }

  @NotNull
  @Override
  public <T> T[] toArray(T[] a) {
    return collection.toArray(a);
  }

}
