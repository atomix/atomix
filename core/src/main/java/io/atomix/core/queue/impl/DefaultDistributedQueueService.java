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
package io.atomix.core.queue.impl;

import io.atomix.core.collection.impl.DefaultDistributedCollectionService;
import io.atomix.core.queue.DistributedQueueType;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;

import java.util.ArrayDeque;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Default distributed queue service.
 */
public class DefaultDistributedQueueService extends DefaultDistributedCollectionService<Queue<String>, String> implements DistributedQueueService {
  public DefaultDistributedQueueService() {
    super(DistributedQueueType.instance(), new ConcurrentLinkedQueue<>());
  }

  private Queue<String> queue() {
    return collection();
  }

  @Override
  public void backup(BackupOutput output) {
    output.writeObject(new ArrayDeque<>(queue()));
  }

  @Override
  public void restore(BackupInput input) {
    collection = new ConcurrentLinkedQueue<>(input.readObject());
  }

  @Override
  public boolean offer(String element) {
    if (queue().offer(element)) {
      added(element);
      return true;
    }
    return false;
  }

  @Override
  public String remove() {
    try {
      String element = queue().remove();
      removed(element);
      return element;
    } catch (NoSuchElementException e) {
      return null;
    }
  }

  @Override
  public String poll() {
    String element = queue().poll();
    if (element != null) {
      removed(element);
    }
    return element;
  }

  @Override
  public String element() {
    try {
      return queue().element();
    } catch (NoSuchElementException e) {
      return null;
    }
  }

  @Override
  public String peek() {
    return queue().peek();
  }
}
