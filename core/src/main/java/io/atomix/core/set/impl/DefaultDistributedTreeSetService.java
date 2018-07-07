/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.set.impl;

import io.atomix.core.set.DistributedTreeSetType;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Serializer;

import java.util.Iterator;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Default distributed tree set service.
 */
public class DefaultDistributedTreeSetService<E extends Comparable<E>> extends AbstractDistributedSetService<NavigableSet<E>, E> implements DistributedTreeSetService<E> {
  private final Serializer serializer;

  public DefaultDistributedTreeSetService() {
    super(DistributedTreeSetType.instance(), new ConcurrentSkipListSet<>());
    this.serializer = Serializer.using(Namespace.builder()
        .register(DistributedTreeSetType.instance().namespace())
        .register(SessionId.class)
        .register(IteratorContext.class)
        .register(SubMapIteratorContext.class)
        .register(DescendingIteratorContext.class)
        .register(DescendingSubMapIteratorContext.class)
        .build());
  }

  @Override
  public Serializer serializer() {
    return serializer;
  }

  @Override
  public int size(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
    if (fromElement != null && toElement != null) {
      return set().subSet(fromElement, fromInclusive, toElement, toInclusive).size();
    } else if (fromElement != null) {
      return set().tailSet(fromElement, fromInclusive).size();
    } else if (toElement != null) {
      return set().headSet(toElement, toInclusive).size();
    } else {
      throw new AssertionError();
    }
  }

  @Override
  public void clear(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
    if (fromElement != null && toElement != null) {
      set().subSet(fromElement, fromInclusive, toElement, toInclusive).clear();
    } else if (fromElement != null) {
      set().tailSet(fromElement, fromInclusive).clear();
    } else if (toElement != null) {
      set().headSet(toElement, toInclusive).clear();
    } else {
      throw new AssertionError();
    }
  }

  @Override
  public E lower(E e) {
    return set().lower(e);
  }

  @Override
  public E floor(E e) {
    return set().floor(e);
  }

  @Override
  public E ceiling(E e) {
    return set().ceiling(e);
  }

  @Override
  public E higher(E e) {
    return set().higher(e);
  }

  @Override
  public E pollFirst() {
    return set().pollFirst();
  }

  @Override
  public E pollLast() {
    return set().pollLast();
  }

  @Override
  public E first() {
    return !set().isEmpty() ? set().first() : null;
  }

  @Override
  public E last() {
    return !set().isEmpty() ? set().last() : null;
  }

  @Override
  public long iterate(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
    iterators.put(getCurrentIndex(), new SubMapIteratorContext(getCurrentSession().sessionId().id(), fromElement, fromInclusive, toElement, toInclusive));
    return getCurrentIndex();
  }

  @Override
  public long iterateDescending(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
    iterators.put(getCurrentIndex(), new DescendingSubMapIteratorContext(getCurrentSession().sessionId().id(), fromElement, fromInclusive, toElement, toInclusive));
    return getCurrentIndex();
  }

  @Override
  public long iterateDescending() {
    iterators.put(getCurrentIndex(), new DescendingIteratorContext(getCurrentSession().sessionId().id()));
    return getCurrentIndex();
  }

  protected class DescendingIteratorContext extends AbstractIteratorContext {
    public DescendingIteratorContext(long sessionId) {
      super(sessionId);
    }

    @Override
    protected Iterator<E> create() {
      return collection().descendingIterator();
    }
  }

  protected class SubMapIteratorContext extends AbstractIteratorContext {
    private final E fromElement;
    private final boolean fromInclusive;
    private final E toElement;
    private final boolean toInclusive;

    SubMapIteratorContext(long sessionId, E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
      super(sessionId);
      this.fromElement = fromElement;
      this.fromInclusive = fromInclusive;
      this.toElement = toElement;
      this.toInclusive = toInclusive;
    }

    @Override
    protected Iterator<E> create() {
      if (fromElement != null && toElement != null) {
        return set().subSet(fromElement, fromInclusive, toElement, toInclusive).iterator();
      } else if (fromElement != null) {
        return set().tailSet(fromElement, fromInclusive).iterator();
      } else if (toElement != null) {
        return set().headSet(toElement, toInclusive).iterator();
      } else {
        throw new AssertionError();
      }
    }
  }

  protected class DescendingSubMapIteratorContext extends AbstractIteratorContext {
    private final E fromElement;
    private final boolean fromInclusive;
    private final E toElement;
    private final boolean toInclusive;

    DescendingSubMapIteratorContext(long sessionId, E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
      super(sessionId);
      this.fromElement = fromElement;
      this.fromInclusive = fromInclusive;
      this.toElement = toElement;
      this.toInclusive = toInclusive;
    }

    @Override
    protected Iterator<E> create() {
      if (fromElement != null && toElement != null) {
        return set().subSet(fromElement, fromInclusive, toElement, toInclusive).descendingIterator();
      } else if (fromElement != null) {
        return set().tailSet(fromElement, fromInclusive).descendingIterator();
      } else if (toElement != null) {
        return set().headSet(toElement, toInclusive).descendingIterator();
      } else {
        throw new AssertionError();
      }
    }
  }
}
