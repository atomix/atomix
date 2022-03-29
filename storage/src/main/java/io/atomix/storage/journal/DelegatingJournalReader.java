// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage.journal;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Journal reader delegate.
 */
public class DelegatingJournalReader<E> implements JournalReader<E> {
  private final JournalReader<E> delegate;

  public DelegatingJournalReader(JournalReader<E> delegate) {
    this.delegate = delegate;
  }

  @Override
  public long getFirstIndex() {
    return delegate.getFirstIndex();
  }

  @Override
  public long getCurrentIndex() {
    return delegate.getCurrentIndex();
  }

  @Override
  public Indexed<E> getCurrentEntry() {
    return delegate.getCurrentEntry();
  }

  @Override
  public long getNextIndex() {
    return delegate.getNextIndex();
  }

  @Override
  public boolean hasNext() {
    return delegate.hasNext();
  }

  @Override
  public Indexed<E> next() {
    return delegate.next();
  }

  @Override
  public void reset() {
    delegate.reset();
  }

  @Override
  public void reset(long index) {
    delegate.reset(index);
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
