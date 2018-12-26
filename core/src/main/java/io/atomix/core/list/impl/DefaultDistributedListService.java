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
package io.atomix.core.list.impl;

import io.atomix.core.collection.impl.CollectionUpdateResult;
import io.atomix.core.collection.impl.DefaultDistributedCollectionService;
import io.atomix.core.iterator.impl.IteratorBatch;
import io.atomix.core.list.DistributedListType;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Serializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static io.atomix.core.collection.impl.CollectionUpdateResult.noop;
import static io.atomix.core.collection.impl.CollectionUpdateResult.ok;

/**
 * Default distributed list service.
 */
public class DefaultDistributedListService extends DefaultDistributedCollectionService<List<String>, String> implements DistributedListService {
  private final Serializer serializer;

  public DefaultDistributedListService() {
    super(DistributedListType.instance(), Collections.synchronizedList(new ArrayList<>()));
    this.serializer = Serializer.using(Namespace.builder()
        .register(DistributedListType.instance().namespace())
        .register(SessionId.class)
        .register(DefaultDistributedCollectionService.IteratorContext.class)
        .register(IteratorContext.class)
        .build());
  }

  @Override
  public Serializer serializer() {
    return serializer;
  }

  private List<String> list() {
    return collection();
  }

  @Override
  public void backup(BackupOutput output) {
    output.writeObject(new ArrayList<>(list()));
  }

  @Override
  public void restore(BackupInput input) {
    collection = Collections.synchronizedList(input.readObject());
  }

  @Override
  public CollectionUpdateResult<Boolean> addAll(int index, Collection<? extends String> c) {
    boolean changed = false;
    for (String element : c) {
      if (add(element).status() == CollectionUpdateResult.Status.OK) {
        changed = true;
      }
    }
    return ok(changed);
  }

  @Override
  public String get(int index) {
    return list().get(index);
  }

  @Override
  public CollectionUpdateResult<String> set(int index, String element) {
    try {
      String value = list().set(index, element);
      if (value != null) {
        removed(value);
      }
      added(element);
      return ok(value);
    } catch (IndexOutOfBoundsException e) {
      return noop();
    }
  }

  @Override
  public CollectionUpdateResult<Void> add(int index, String element) {
    try {
      list().add(index, element);
      added(element);
      return ok();
    } catch (IndexOutOfBoundsException e) {
      return noop();
    }
  }

  @Override
  public CollectionUpdateResult<String> remove(int index) {
    try {
      String value = list().remove(index);
      if (value != null) {
        removed(value);
      }
      return ok(value);
    } catch (IndexOutOfBoundsException e) {
      return noop();
    }
  }

  @Override
  public int indexOf(Object o) {
    return list().indexOf(o);
  }

  @Override
  public int lastIndexOf(Object o) {
    return list().lastIndexOf(o);
  }

  @Override
  public IteratorBatch<String> iterate() {
    return iterate(IteratorContext::new);
  }

  protected class IteratorContext extends AbstractIteratorContext {
    public IteratorContext(long sessionId) {
      super(sessionId);
    }

    @Override
    protected Iterator<String> create() {
      return new ListIterator();
    }
  }

  private class ListIterator implements Iterator<String> {
    private int index;

    @Override
    public boolean hasNext() {
      return list().size() > index;
    }

    @Override
    public String next() {
      return list().get(index++);
    }
  }
}
