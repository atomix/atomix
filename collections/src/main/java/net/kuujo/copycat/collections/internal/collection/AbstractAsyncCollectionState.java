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

import net.kuujo.copycat.StateContext;

import java.util.Collection;

/**
 * Abstract asynchronous collection state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractAsyncCollectionState<T extends AsyncCollectionState<T, U>, U> implements AsyncCollectionState<T, U> {

  /**
   * Creates the collection.
   */
  protected abstract Collection<U> createCollection();

  /**
   * Gets the collection from the state context.
   */
  private Collection<U> getCollection(StateContext<T> context) {
    Collection<U> collection = context.get("value");
    if (collection == null) {
      collection = createCollection();
      context.put("value", collection);
    }
    return collection;
  }

  @Override
  public boolean add(U value, StateContext<T> context) {
    return getCollection(context).add(value);
  }

  @Override
  public boolean remove(U value, StateContext<T> context) {
    return getCollection(context).remove(value);
  }

  @Override
  public boolean contains(Object value, StateContext<T> context) {
    return getCollection(context).contains(value);
  }

  @Override
  public int size(StateContext<T> context) {
    return getCollection(context).size();
  }

  @Override
  public boolean isEmpty(StateContext<T> context) {
    return getCollection(context).isEmpty();
  }

  @Override
  public void clear(StateContext<T> context) {
    getCollection(context).clear();
  }

}
