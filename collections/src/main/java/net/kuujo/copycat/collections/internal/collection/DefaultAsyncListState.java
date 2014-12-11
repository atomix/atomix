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

import java.util.ArrayList;
import java.util.List;

/**
 * Default asynchronous list state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultAsyncListState<T> extends AbstractAsyncCollectionState<AsyncListState<T>, T> implements AsyncListState<T> {

  @Override
  protected List<T> createCollection() {
    return new ArrayList<>();
  }

  /**
   * Gets the list from the current state context.
   */
  private List<T> getList(StateContext<AsyncListState<T>> context) {
    List<T> list = context.get("value");
    if (list == null) {
      list = createCollection();
      context.put("value", list);
    }
    return list;
  }

  @Override
  public T get(int index, StateContext<AsyncListState<T>> context) {
    return getList(context).get(index);
  }

  @Override
  public void set(int index, T value, StateContext<AsyncListState<T>> context) {
    getList(context).set(index, value);
  }

  @Override
  public T remove(int index, StateContext<AsyncListState<T>> context) {
    return getList(context).remove(index);
  }

}
