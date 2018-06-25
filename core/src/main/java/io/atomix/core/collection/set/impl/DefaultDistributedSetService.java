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
package io.atomix.core.collection.set.impl;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.core.collection.impl.CollectionUpdateResult;
import io.atomix.core.collection.impl.DefaultDistributedCollectionService;
import io.atomix.core.collection.set.DistributedSetType;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.core.transaction.impl.CommitResult;
import io.atomix.core.transaction.impl.PrepareResult;
import io.atomix.core.transaction.impl.RollbackResult;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static io.atomix.core.collection.impl.CollectionUpdateResult.writeLockConflict;

/**
 * Default distributed set service.
 */
public class DefaultDistributedSetService extends DefaultDistributedCollectionService<Set<String>> implements DistributedSetService {
  private Set<String> lockedElements = Sets.newHashSet();
  private Map<TransactionId, TransactionLog<SetUpdate<String>>> transactions = Maps.newHashMap();

  public DefaultDistributedSetService() {
    super(DistributedSetType.instance(), Sets.newConcurrentHashSet());
  }

  private Set<String> set() {
    return collection();
  }

  @Override
  public void backup(BackupOutput output) {
    super.backup(output);
    output.writeObject(lockedElements);
    output.writeObject(transactions);
  }

  @Override
  public void restore(BackupInput input) {
    super.restore(input);
    lockedElements = input.readObject();
    transactions = input.readObject();
  }

  @Override
  public CollectionUpdateResult<Boolean> add(String element) {
    if (lockedElements.contains(element)) {
      return writeLockConflict();
    }
    return super.add(element);
  }

  @Override
  public CollectionUpdateResult<Boolean> remove(Object element) {
    if (lockedElements.contains(element)) {
      return writeLockConflict();
    }
    return super.remove(element);
  }

  @Override
  public CollectionUpdateResult<Boolean> addAll(Collection<? extends String> c) {
    for (String element : c) {
      if (lockedElements.contains(element)) {
        return writeLockConflict();
      }
    }
    return super.addAll(c);
  }

  @Override
  public CollectionUpdateResult<Boolean> retainAll(Collection<?> c) {
    for (String element : set()) {
      if (lockedElements.contains(element) && !c.contains(element)) {
        return writeLockConflict();
      }
    }
    return super.retainAll(c);
  }

  @Override
  public CollectionUpdateResult<Boolean> removeAll(Collection<?> c) {
    for (Object element : c) {
      if (lockedElements.contains(element)) {
        return writeLockConflict();
      }
    }
    return super.removeAll(c);
  }

  @Override
  public CollectionUpdateResult<Void> clear() {
    if (!lockedElements.isEmpty()) {
      return writeLockConflict();
    }
    return super.clear();
  }

  @Override
  public PrepareResult prepareAndCommit(TransactionLog<SetUpdate<String>> transactionLog) {
    PrepareResult result = prepare(transactionLog);
    if (result == PrepareResult.OK) {
      commit(transactionLog.transactionId());
    }
    return result;
  }

  @Override
  public PrepareResult prepare(TransactionLog<SetUpdate<String>> transactionLog) {
    for (SetUpdate<String> update : transactionLog.records()) {
      if (lockedElements.contains(update.element())) {
        return PrepareResult.CONCURRENT_TRANSACTION;
      }
    }

    for (SetUpdate<String> update : transactionLog.records()) {
      String element = update.element();
      switch (update.type()) {
        case ADD:
        case NOT_CONTAINS:
          if (set().contains(element)) {
            return PrepareResult.OPTIMISTIC_LOCK_FAILURE;
          }
          break;
        case REMOVE:
        case CONTAINS:
          if (!set().contains(element)) {
            return PrepareResult.OPTIMISTIC_LOCK_FAILURE;
          }
          break;
      }
    }

    for (SetUpdate<String> update : transactionLog.records()) {
      lockedElements.add(update.element());
    }
    transactions.put(transactionLog.transactionId(), transactionLog);
    return PrepareResult.OK;
  }

  @Override
  public CommitResult commit(TransactionId transactionId) {
    TransactionLog<SetUpdate<String>> transactionLog = transactions.remove(transactionId);
    if (transactionLog == null) {
      return CommitResult.UNKNOWN_TRANSACTION_ID;
    }

    for (SetUpdate<String> update : transactionLog.records()) {
      switch (update.type()) {
        case ADD:
          set().add(update.element());
          break;
        case REMOVE:
          set().remove(update.element());
          break;
        default:
          break;
      }
      lockedElements.remove(update.element());
    }
    return CommitResult.OK;
  }

  @Override
  public RollbackResult rollback(TransactionId transactionId) {
    TransactionLog<SetUpdate<String>> transactionLog = transactions.remove(transactionId);
    if (transactionLog == null) {
      return RollbackResult.UNKNOWN_TRANSACTION_ID;
    }

    for (SetUpdate<String> update : transactionLog.records()) {
      lockedElements.remove(update.element());
    }
    return RollbackResult.OK;
  }
}
