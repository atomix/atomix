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
package io.atomix.core.multiset.impl;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import io.atomix.core.collection.impl.CollectionUpdateResult;
import io.atomix.core.collection.impl.DefaultDistributedCollectionService;
import io.atomix.core.iterator.impl.IteratorBatch;
import io.atomix.core.multiset.DistributedMultisetType;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Serializer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.atomix.core.collection.impl.CollectionUpdateResult.ok;

/**
 * Default distributed multiset service.
 */
public class DefaultDistributedMultisetService extends DefaultDistributedCollectionService<Multiset<String>, String> implements DistributedMultisetService {
  private static final int MAX_ITERATOR_BATCH_SIZE = 1024 * 32;

  private final Serializer serializer;
  protected Map<Long, IteratorContext> entryIterators = Maps.newHashMap();

  public DefaultDistributedMultisetService() {
    super(DistributedMultisetType.instance(), HashMultiset.create());
    this.serializer = Serializer.using(Namespace.builder()
        .register(DistributedMultisetType.instance().namespace())
        .register(SessionId.class)
        .register(DefaultDistributedCollectionService.IteratorContext.class)
        .register(IteratorContext.class)
        .build());
  }

  @Override
  public Serializer serializer() {
    return serializer;
  }

  private Multiset<String> multiset() {
    return collection();
  }

  @Override
  public void backup(BackupOutput output) {
    super.backup(output);
    output.writeObject(entryIterators);
  }

  @Override
  public void restore(BackupInput input) {
    super.restore(input);
    entryIterators = input.readObject();
  }

  @Override
  public void onExpire(Session session) {
    super.onExpire(session);
    entryIterators.entrySet().removeIf(entry -> entry.getValue().sessionId == session.sessionId().id());
  }

  @Override
  public void onClose(Session session) {
    super.onClose(session);
    entryIterators.entrySet().removeIf(entry -> entry.getValue().sessionId == session.sessionId().id());
  }

  @Override
  public int count(Object element) {
    return multiset().count(element);
  }

  @Override
  public CollectionUpdateResult<Integer> add(String element, int occurrences) {
    int count = multiset().add(element, occurrences);
    for (int i = 0; i < occurrences; i++) {
      added(element);
    }
    return ok(count);
  }

  @Override
  public CollectionUpdateResult<Integer> remove(Object element, int occurrences) {
    int count = multiset().remove(element, occurrences);
    for (int i = 0; i < Math.min(count, occurrences); i++) {
      removed((String) element);
    }
    return ok(count);
  }

  @Override
  public CollectionUpdateResult<Integer> setCount(String element, int count) {
    int previous = multiset().setCount(element, count);
    if (previous < count) {
      for (int i = previous; i < count; i++) {
        added(element);
      }
    } else if (previous > count) {
      for (int i = count; i < previous; i++) {
        removed(element);
      }
    }
    return ok(previous);
  }

  @Override
  public CollectionUpdateResult<Boolean> setCount(String element, int oldCount, int newCount) {
    boolean succeeded = multiset().setCount(element, oldCount, newCount);
    if (succeeded) {
      if (oldCount < newCount) {
        for (int i = oldCount; i < newCount; i++) {
          added(element);
        }
      } else if (oldCount > newCount) {
        for (int i = newCount; i < oldCount; i++) {
          removed(element);
        }
      }
    }
    return ok(succeeded);
  }

  @Override
  public int elements() {
    return multiset().elementSet().size();
  }

  @Override
  public IteratorBatch<String> iterateElements() {
    IteratorBatch<Multiset.Entry<String>> batch = iterateEntries();
    return batch == null ? null : new IteratorBatch<>(batch.id(), batch.position(), batch.entries()
        .stream()
        .map(element -> element.getElement())
        .collect(Collectors.toList()), batch.complete());
  }

  @Override
  public IteratorBatch<String> nextElements(long iteratorId, int position) {
    IteratorBatch<Multiset.Entry<String>> batch = nextEntries(iteratorId, position);
    return batch == null ? null : new IteratorBatch<>(batch.id(), batch.position(), batch.entries()
        .stream()
        .map(element -> element.getElement())
        .collect(Collectors.toList()), batch.complete());
  }

  @Override
  public void closeElements(long iteratorId) {
    closeEntries(iteratorId);
  }

  @Override
  public IteratorBatch<Multiset.Entry<String>> iterateEntries() {
    IteratorContext iterator = new IteratorContext(getCurrentSession().sessionId().id());
    if (!iterator.iterator.hasNext()) {
      return null;
    }

    long iteratorId = getCurrentIndex();
    entryIterators.put(iteratorId, iterator);
    IteratorBatch<Multiset.Entry<String>> batch = nextEntries(iteratorId, 0);
    if (batch.complete()) {
      entryIterators.remove(iteratorId);
    }
    return batch;
  }

  @Override
  public IteratorBatch<Multiset.Entry<String>> nextEntries(long iteratorId, int position) {
    IteratorContext context = entryIterators.get(iteratorId);
    if (context == null) {
      return null;
    }

    List<Multiset.Entry<String>> entries = new ArrayList<>();
    int size = 0;
    while (context.iterator.hasNext()) {
      context.position++;
      if (context.position > position) {
        Multiset.Entry<String> entry = context.iterator.next();
        entries.add(Multisets.immutableEntry(entry.getElement(), entry.getCount()));
        size += entry.getElement().length() + 4;

        if (size >= MAX_ITERATOR_BATCH_SIZE) {
          break;
        }
      }
    }

    if (entries.isEmpty()) {
      return null;
    }
    return new IteratorBatch<>(iteratorId, context.position, entries, !context.iterator.hasNext());
  }

  @Override
  public void closeEntries(long iteratorId) {
    entryIterators.remove(iteratorId);
  }

  private class IteratorContext {
    private final long sessionId;
    private int position = 0;
    private transient Iterator<Multiset.Entry<String>> iterator = multiset().entrySet().iterator();

    IteratorContext(long sessionId) {
      this.sessionId = sessionId;
    }
  }
}
