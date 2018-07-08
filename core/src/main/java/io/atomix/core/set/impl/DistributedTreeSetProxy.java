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

import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.collection.impl.DistributedCollectionProxy;
import io.atomix.core.iterator.AsyncIterator;
import io.atomix.core.iterator.impl.ProxyIterator;
import io.atomix.core.set.AsyncDistributedNavigableSet;
import io.atomix.core.set.AsyncDistributedSortedSet;
import io.atomix.core.set.AsyncDistributedTreeSet;
import io.atomix.core.set.DistributedNavigableSet;
import io.atomix.core.set.DistributedTreeSet;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.utils.concurrent.Futures;

import java.time.Duration;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Distributed tree set proxy.
 */
public class DistributedTreeSetProxy<E extends Comparable<E>>
    extends DistributedCollectionProxy<AsyncDistributedTreeSet<E>, DistributedTreeSetService<E>, E>
    implements AsyncDistributedTreeSet<E> {

  public DistributedTreeSetProxy(ProxyClient<DistributedTreeSetService<E>> client, PrimitiveRegistry registry) {
    super(client, registry);
  }

  @Override
  public CompletableFuture<E> lower(E e) {
    return getProxyClient().applyBy(name(), service -> service.lower(e));
  }

  @Override
  public CompletableFuture<E> floor(E e) {
    return getProxyClient().applyBy(name(), service -> service.floor(e));
  }

  @Override
  public CompletableFuture<E> ceiling(E e) {
    return getProxyClient().applyBy(name(), service -> service.ceiling(e));
  }

  @Override
  public CompletableFuture<E> higher(E e) {
    return getProxyClient().applyBy(name(), service -> service.higher(e));
  }

  @Override
  public CompletableFuture<E> pollFirst() {
    return getProxyClient().applyBy(name(), service -> service.pollFirst());
  }

  @Override
  public CompletableFuture<E> pollLast() {
    return getProxyClient().applyBy(name(), service -> service.pollLast());
  }

  @Override
  public AsyncDistributedNavigableSet<E> descendingSet() {
    return new DescendingAsyncDistributedNavigableSet<>(this);
  }

  @Override
  public AsyncIterator<E> descendingIterator() {
    return new ProxyIterator<>(
        getProxyClient(),
        getProxyClient().getPartitionId(name()),
        DistributedTreeSetService::iterateDescending,
        DistributedTreeSetService::next,
        DistributedTreeSetService::close);
  }

  @Override
  public AsyncDistributedNavigableSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
    return new SubSet(fromElement, fromInclusive, toElement, toInclusive);
  }

  @Override
  public AsyncDistributedNavigableSet<E> headSet(E toElement, boolean inclusive) {
    return new SubSet(null, false, toElement, inclusive);
  }

  @Override
  public AsyncDistributedNavigableSet<E> tailSet(E fromElement, boolean inclusive) {
    return new SubSet(fromElement, inclusive, null, false);
  }

  @Override
  public AsyncDistributedSortedSet<E> subSet(E fromElement, E toElement) {
    return subSet(fromElement, true, toElement, false);
  }

  @Override
  public AsyncDistributedSortedSet<E> headSet(E toElement) {
    return headSet(toElement, false);
  }

  @Override
  public AsyncDistributedSortedSet<E> tailSet(E fromElement) {
    return tailSet(fromElement, true);
  }

  @Override
  public CompletableFuture<E> first() {
    return getProxyClient().applyBy(name(), service -> service.first())
        .thenCompose(value -> value != null ? CompletableFuture.completedFuture(value) : Futures.exceptionalFuture(new NoSuchElementException()));
  }

  @Override
  public CompletableFuture<E> last() {
    return getProxyClient().applyBy(name(), service -> service.last())
        .thenCompose(value -> value != null ? CompletableFuture.completedFuture(value) : Futures.exceptionalFuture(new NoSuchElementException()));
  }

  @Override
  public CompletableFuture<Boolean> prepare(TransactionLog<SetUpdate<E>> transactionLog) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Void> commit(TransactionId transactionId) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Void> rollback(TransactionId transactionId) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public DistributedTreeSet<E> sync(Duration operationTimeout) {
    return new BlockingDistributedTreeSet<>(this, operationTimeout.toMillis());
  }

  private class SubSet implements AsyncDistributedNavigableSet<E> {
    protected final E fromElement;
    protected final boolean fromInclusive;
    protected final E toElement;
    protected final boolean toInclusive;

    SubSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
      this.fromElement = fromElement;
      this.fromInclusive = fromInclusive;
      this.toElement = toElement;
      this.toInclusive = toInclusive;
    }

    @Override
    public String name() {
      return DistributedTreeSetProxy.this.name();
    }

    @Override
    public PrimitiveType type() {
      return DistributedTreeSetProxy.this.type();
    }

    @Override
    public PrimitiveProtocol protocol() {
      return DistributedTreeSetProxy.this.protocol();
    }

    private boolean isInBounds(E element) {
      if (fromElement != null) {
        int lower = element.compareTo(fromElement);
        if (!fromInclusive && lower <= 0 || fromInclusive && lower < 0) {
          return false;
        }
      }
      if (toElement != null) {
        int upper = element.compareTo(toElement);
        if (!toInclusive && upper >= 0 || toInclusive && upper > 0) {
          return false;
        }
      }
      return true;
    }

    @Override
    public CompletableFuture<E> pollFirst() {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<E> pollLast() {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<E> first() {
      if (fromElement == null) {
        return DistributedTreeSetProxy.this.first();
      } else if (fromInclusive) {
        return DistributedTreeSetProxy.this.ceiling(fromElement);
      } else {
        return DistributedTreeSetProxy.this.higher(fromElement);
      }
    }

    @Override
    public CompletableFuture<E> last() {
      if (toElement == null) {
        return DistributedTreeSetProxy.this.last();
      } else if (toInclusive) {
        return DistributedTreeSetProxy.this.floor(toElement);
      } else {
        return DistributedTreeSetProxy.this.lower(toElement);
      }
    }

    @Override
    public CompletableFuture<E> lower(E e) {
      if (toElement == null) {
        return DistributedTreeSetProxy.this.lower(e);
      } else if (toInclusive && toElement.compareTo(e) < 0) {
        return DistributedTreeSetProxy.this.floor(toElement).thenApply(result -> isInBounds(result) ? result : null);
      } else {
        return DistributedTreeSetProxy.this.lower(min(toElement, e)).thenApply(result -> isInBounds(result) ? result : null);
      }
    }

    @Override
    public CompletableFuture<E> floor(E e) {
      if (toElement == null) {
        return DistributedTreeSetProxy.this.floor(e);
      } else if (!toInclusive && toElement.compareTo(e) <= 0) {
        return DistributedTreeSetProxy.this.lower(toElement).thenApply(result -> isInBounds(result) ? result : null);
      } else {
        return DistributedTreeSetProxy.this.floor(min(toElement, e)).thenApply(result -> isInBounds(result) ? result : null);
      }
    }

    @Override
    public CompletableFuture<E> ceiling(E e) {
      if (fromElement == null) {
        return DistributedTreeSetProxy.this.ceiling(e);
      } else if (!fromInclusive && fromElement.compareTo(e) >= 0) {
        return DistributedTreeSetProxy.this.higher(fromElement).thenApply(result -> isInBounds(result) ? result : null);
      } else {
        return DistributedTreeSetProxy.this.ceiling(max(fromElement, e)).thenApply(result -> isInBounds(result) ? result : null);
      }
    }

    @Override
    public CompletableFuture<E> higher(E e) {
      if (fromElement == null) {
        return DistributedTreeSetProxy.this.higher(e);
      } else if (fromInclusive && fromElement.compareTo(e) > 0) {
        return DistributedTreeSetProxy.this.ceiling(fromElement).thenApply(result -> isInBounds(result) ? result : null);
      } else {
        return DistributedTreeSetProxy.this.higher(max(fromElement, e)).thenApply(result -> isInBounds(result) ? result : null);
      }
    }

    @Override
    public AsyncDistributedNavigableSet<E> descendingSet() {
      return new DescendingAsyncDistributedNavigableSet<>(this);
    }

    @Override
    public AsyncDistributedSortedSet<E> subSet(E fromElement, E toElement) {
      return subSet(fromElement, true, toElement, false);
    }

    @Override
    public AsyncDistributedSortedSet<E> headSet(E toElement) {
      return headSet(toElement, false);
    }

    @Override
    public AsyncDistributedSortedSet<E> tailSet(E fromElement) {
      return tailSet(fromElement, true);
    }

    @Override
    public AsyncDistributedNavigableSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
      checkNotNull(fromElement);
      checkNotNull(toElement);

      if (this.fromElement != null) {
        int order = this.fromElement.compareTo(fromElement);
        if (order == 0) {
          fromInclusive = this.fromInclusive && fromInclusive;
        } else if (order > 0) {
          fromInclusive = this.fromInclusive;
        }
      }

      if (this.toElement != null) {
        int order = this.toElement.compareTo(toElement);
        if (order == 0) {
          toInclusive = this.toInclusive && toInclusive;
        } else if (order > 0) {
          toInclusive = this.toInclusive;
        }
      }
      return DistributedTreeSetProxy.this.subSet(fromElement, fromInclusive, toElement, toInclusive);
    }

    @Override
    public AsyncDistributedNavigableSet<E> headSet(E toElement, boolean inclusive) {
      checkNotNull(toElement);

      if (this.toElement != null) {
        int order = this.toElement.compareTo(toElement);
        if (order == 0) {
          inclusive = this.toInclusive && inclusive;
        } else if (order > 0) {
          inclusive = this.toInclusive;
        }
      }
      return DistributedTreeSetProxy.this.subSet(fromElement, fromInclusive, toElement, inclusive);
    }

    @Override
    public AsyncDistributedNavigableSet<E> tailSet(E fromElement, boolean inclusive) {
      checkNotNull(fromElement);

      if (this.fromElement != null) {
        int order = this.fromElement.compareTo(fromElement);
        if (order == 0) {
          inclusive = this.fromInclusive && inclusive;
        } else if (order > 0) {
          inclusive = this.fromInclusive;
        }
      }
      return DistributedTreeSetProxy.this.subSet(fromElement, inclusive, toElement, toInclusive);
    }

    @Override
    public CompletableFuture<Boolean> add(E element) {
      return DistributedTreeSetProxy.this.add(element);
    }

    @Override
    public CompletableFuture<Boolean> remove(E element) {
      return DistributedTreeSetProxy.this.remove(element);
    }

    @Override
    public CompletableFuture<Integer> size() {
      return getProxyClient().applyBy(name(), service -> service.size(fromElement, fromInclusive, toElement, toInclusive));
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return size().thenApply(size -> size == 0);
    }

    @Override
    public CompletableFuture<Void> clear() {
      return getProxyClient().acceptBy(name(), service -> service.clear(fromElement, fromInclusive, toElement, toInclusive));
    }

    @Override
    public CompletableFuture<Boolean> contains(E element) {
      return isInBounds(element) ? DistributedTreeSetProxy.this.contains(element) : CompletableFuture.completedFuture(false);
    }

    @Override
    public CompletableFuture<Boolean> addAll(Collection<? extends E> c) {
      return DistributedTreeSetProxy.this.addAll(c.stream().filter(this::isInBounds).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Boolean> containsAll(Collection<? extends E> c) {
      if (c.stream().map(this::isInBounds).reduce(Boolean::logicalAnd).orElse(true)) {
        return DistributedTreeSetProxy.this.containsAll(c);
      }
      return CompletableFuture.completedFuture(false);
    }

    @Override
    public CompletableFuture<Boolean> retainAll(Collection<? extends E> c) {
      return DistributedTreeSetProxy.this.retainAll(c.stream().filter(this::isInBounds).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Boolean> removeAll(Collection<? extends E> c) {
      return DistributedTreeSetProxy.this.removeAll(c.stream().filter(this::isInBounds).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Void> addListener(CollectionEventListener<E> listener) {
      return DistributedTreeSetProxy.this.addListener(listener);
    }

    @Override
    public CompletableFuture<Void> removeListener(CollectionEventListener<E> listener) {
      return DistributedTreeSetProxy.this.removeListener(listener);
    }

    @Override
    public AsyncIterator<E> iterator() {
      return new ProxyIterator<>(
          getProxyClient(),
          getProxyClient().getPartitionId(name()),
          service -> service.iterate(fromElement, fromInclusive, toElement, toInclusive),
          DistributedTreeSetService::next,
          DistributedTreeSetService::close);
    }

    @Override
    public AsyncIterator<E> descendingIterator() {
      return new ProxyIterator<>(
          getProxyClient(),
          getProxyClient().getPartitionId(name()),
          service -> service.iterateDescending(fromElement, fromInclusive, toElement, toInclusive),
          DistributedTreeSetService::next,
          DistributedTreeSetService::close);
    }

    @Override
    public CompletableFuture<Boolean> prepare(TransactionLog<SetUpdate<E>> transactionLog) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Void> commit(TransactionId transactionId) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Void> rollback(TransactionId transactionId) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Void> close() {
      return DistributedTreeSetProxy.this.close();
    }

    @Override
    public DistributedNavigableSet<E> sync(Duration operationTimeout) {
      return new BlockingDistributedNavigableSet<>(this, operationTimeout.toMillis());
    }

    private E min(E a, E b) {
      if (a == null) {
        return b;
      } else if (b == null) {
        return a;
      }
      return a.compareTo(b) < 0 ? a : b;
    }

    private E max(E a, E b) {
      if (a == null) {
        return b;
      } else if (b == null) {
        return a;
      }
      return a.compareTo(b) > 0 ? a : b;
    }
  }
}
