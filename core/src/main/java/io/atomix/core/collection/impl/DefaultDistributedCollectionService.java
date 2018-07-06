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
package io.atomix.core.collection.impl;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.core.collection.CollectionEvent;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Serializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.atomix.core.collection.impl.CollectionUpdateResult.noop;
import static io.atomix.core.collection.impl.CollectionUpdateResult.ok;

/**
 * Default distributed collection service.
 */
public abstract class DefaultDistributedCollectionService<T extends Collection<String>>
    extends AbstractPrimitiveService<DistributedCollectionClient>
    implements DistributedCollectionService {

  private static final int MAX_ITERATOR_BATCH_SIZE = 1024 * 32;

  private final Serializer serializer;
  private T collection;
  private Map<Long, IteratorContext> iterators = Maps.newHashMap();
  private Set<SessionId> listeners = Sets.newHashSet();

  protected DefaultDistributedCollectionService(PrimitiveType primitiveType, T collection) {
    super(primitiveType, DistributedCollectionClient.class);
    this.collection = collection;
    this.serializer = Serializer.using(Namespace.builder()
        .register(primitiveType.namespace())
        .register(SessionId.class)
        .register(IteratorContext.class)
        .build());
  }

  @Override
  public Serializer serializer() {
    return serializer;
  }

  /**
   * Returns the collection instance.
   *
   * @return the collection instance
   */
  protected T collection() {
    return collection;
  }

  @Override
  public void backup(BackupOutput output) {
    output.writeObject(collection);
  }

  @Override
  public void restore(BackupInput input) {
    collection = input.readObject();
  }

  protected void added(String element) {
    listeners.forEach(l -> getSession(l).accept(client -> client.onEvent(new CollectionEvent<>(CollectionEvent.Type.ADD, element))));
  }

  protected void removed(String element) {
    listeners.forEach(l -> getSession(l).accept(client -> client.onEvent(new CollectionEvent<>(CollectionEvent.Type.REMOVE, element))));
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
  public boolean contains(Object o) {
    return collection.contains(o);
  }

  @Override
  public CollectionUpdateResult<Boolean> add(String element) {
    if (collection.add(element)) {
      added(element);
      return ok(true);
    }
    return noop(false);
  }

  @Override
  public CollectionUpdateResult<Boolean> remove(Object element) {
    if (collection.remove(element)) {
      removed((String) element);
      return ok(true);
    }
    return noop(false);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return collection.containsAll(c);
  }

  @Override
  public CollectionUpdateResult<Boolean> addAll(Collection<? extends String> c) {
    boolean changed = false;
    for (String element : c) {
      if (add(element).status() == CollectionUpdateResult.Status.OK) {
        changed = true;
      }
    }
    return ok(changed);
  }

  @Override
  public CollectionUpdateResult<Boolean> retainAll(Collection<?> c) {
    boolean changed = false;
    for (String element : collection) {
      if (!c.contains(element) && remove(element).status() == CollectionUpdateResult.Status.OK) {
        changed = true;
      }
    }
    return ok(changed);
  }

  @Override
  public CollectionUpdateResult<Boolean> removeAll(Collection<?> c) {
    boolean changed = false;
    for (Object element : c) {
      if (remove(element).status() == CollectionUpdateResult.Status.OK) {
        changed = true;
      }
    }
    return ok(changed);
  }

  @Override
  public CollectionUpdateResult<Void> clear() {
    collection.forEach(element -> removed(element));
    collection.clear();
    return ok();
  }

  @Override
  public void listen() {
    listeners.add(getCurrentSession().sessionId());
  }

  @Override
  public void unlisten() {
    listeners.remove(getCurrentSession().sessionId());
  }

  @Override
  public long iterate() {
    iterators.put(getCurrentIndex(), new IteratorContext(getCurrentSession().sessionId().id()));
    return getCurrentIndex();
  }

  @Override
  public IteratorBatch<String> next(long iteratorId, int position) {
    IteratorContext context = iterators.get(iteratorId);
    if (context == null) {
      return null;
    }

    List<String> elements = new ArrayList<>();
    int size = 0;
    while (context.iterator.hasNext()) {
      context.position++;
      if (context.position > position) {
        String element = context.iterator.next();
        size += element.length();
        elements.add(element);

        if (size >= MAX_ITERATOR_BATCH_SIZE) {
          break;
        }
      }
    }

    if (elements.isEmpty()) {
      return null;
    }
    return new IteratorBatch<>(context.position, elements);
  }

  @Override
  public void close(long iteratorId) {
    iterators.remove(iteratorId);
  }

  @Override
  public void onExpire(Session session) {
    listeners.remove(session.sessionId());
    iterators.entrySet().removeIf(entry -> entry.getValue().sessionId == session.sessionId().id());
  }

  @Override
  public void onClose(Session session) {
    listeners.remove(session.sessionId());
    iterators.entrySet().removeIf(entry -> entry.getValue().sessionId == session.sessionId().id());
  }

  protected class IteratorContext {
    private final long sessionId;
    private int position = 0;
    private transient Iterator<String> iterator = collection().iterator();

    IteratorContext(long sessionId) {
      this.sessionId = sessionId;
    }
  }
}
