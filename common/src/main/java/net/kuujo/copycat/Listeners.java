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
package net.kuujo.copycat;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Utility for managing a set of listeners.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Listeners<T> implements Iterable<ListenerContext<T>> {
  private final List<ListenerContext<T>> listeners = new CopyOnWriteArrayList<>();

  /**
   * Adds a listener to the set of listeners.
   *
   * @param listener The listener to add.
   * @return The listener context.
   */
  public ListenerContext<T> add(Listener<T> listener) {
    if (listener == null)
      throw new NullPointerException("listener cannot be null");

    ListenerContext<T> context = new ListenerContext<T>() {
      @Override
      public void accept(T event) {
        listener.accept(event);
      }

      @Override
      public void close() {
        listeners.remove(this);
      }
    };

    listeners.add(context);
    return context;
  }

  @Override
  public Iterator<ListenerContext<T>> iterator() {
    return listeners.iterator();
  }

}
