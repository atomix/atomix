// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage.journal;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Indexed journal entry.
 */
public class Indexed<E> {
  private final long index;
  private final E entry;
  private final int size;

  public Indexed(long index, E entry, int size) {
    this.index = index;
    this.entry = entry;
    this.size = size;
  }

  /**
   * Returns the entry index.
   *
   * @return The entry index.
   */
  public long index() {
    return index;
  }

  /**
   * Returns the indexed entry.
   *
   * @return The indexed entry.
   */
  public E entry() {
    return entry;
  }

  /**
   * Returns the serialized entry size.
   *
   * @return The serialized entry size.
   */
  public int size() {
    return size;
  }

  /**
   * Returns the entry type class.
   *
   * @return The entry class.
   */
  public Class<?> type() {
    return entry.getClass();
  }

  /**
   * Casts the entry to the given type.
   *
   * @return The cast entry.
   */
  @SuppressWarnings("unchecked")
  public <E> Indexed<E> cast() {
    return (Indexed<E>) this;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("index", index)
        .add("entry", entry)
        .toString();
  }
}
