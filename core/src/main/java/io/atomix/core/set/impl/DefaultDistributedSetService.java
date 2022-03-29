// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.set.impl;

import com.google.common.collect.Sets;
import io.atomix.core.set.DistributedSetType;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;

import java.util.Set;

/**
 * Default distributed set service.
 */
public class DefaultDistributedSetService<E> extends AbstractDistributedSetService<Set<E>, E> implements DistributedSetService<E> {
  public DefaultDistributedSetService() {
    super(DistributedSetType.instance(), Sets.newConcurrentHashSet());
  }

  @Override
  public void backup(BackupOutput output) {
    output.writeObject(Sets.newHashSet(collection));
    output.writeObject(lockedElements);
    output.writeObject(transactions);
  }

  @Override
  public void restore(BackupInput input) {
    collection = Sets.newConcurrentHashSet(input.readObject());
    lockedElements = input.readObject();
    transactions = input.readObject();
  }
}
