// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage.journal;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Delegating journal.
 */
public class DelegatingJournal<E> implements Journal<E> {
  private final Journal<E> delegate;

  public DelegatingJournal(Journal<E> delegate) {
    this.delegate = delegate;
  }

  @Override
  public JournalWriter<E> writer() {
    return delegate.writer();
  }

  @Override
  public JournalReader<E> openReader(long index) {
    return delegate.openReader(index);
  }

  @Override
  public JournalReader<E> openReader(long index, JournalReader.Mode mode) {
    return delegate.openReader(index, mode);
  }

  @Override
  public boolean isOpen() {
    return delegate.isOpen();
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("delegate", delegate)
        .toString();
  }
}
