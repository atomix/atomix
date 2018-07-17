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
import io.atomix.core.list.DistributedListType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static io.atomix.core.collection.impl.CollectionUpdateResult.noop;
import static io.atomix.core.collection.impl.CollectionUpdateResult.ok;

/**
 * Default distributed list service.
 */
public class DefaultDistributedListService extends DefaultDistributedCollectionService<List<String>, String> implements DistributedListService {
  public DefaultDistributedListService() {
    super(DistributedListType.instance(), Collections.synchronizedList(new ArrayList<>()));
  }

  private List<String> list() {
    return collection();
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
}
