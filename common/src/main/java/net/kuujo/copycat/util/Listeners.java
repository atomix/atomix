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
package net.kuujo.copycat.util;

import net.kuujo.copycat.util.concurrent.Context;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Utility for managing a set of listeners.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Listeners<T> implements Iterable<Listener<T>> {
  private final List<Listener<T>> listeners = new CopyOnWriteArrayList<>();

  /**
   * Returns the number of registered listeners.
   *
   * @return The number of registered listeners.
   */
  public int size() {
    return listeners.size();
  }

  /**
   * Adds a listener to the set of listeners.
   *
   * @param listener The listener to add.
   * @return The listener context.
   * @throws NullPointerException if {@code listener} is null
   */
  public Listener<T> add(Consumer<T> listener) {
    Assert.notNull(listener, "listener");
    
    Context context = Context.currentContext();

    Listener<T> wrapper = new Listener<T>() {
      @Override
      public void accept(T event) {
        if (context != null) {
          context.executor().execute(() -> listener.accept(event));
        } else {
          listener.accept(event);
        }
      }

      @Override
      public void close() {
        listeners.remove(this);
      }
    };

    listeners.add(wrapper);
    return wrapper;
  }

  @Override
  public Iterator<Listener<T>> iterator() {
    return listeners.iterator();
  }

}
