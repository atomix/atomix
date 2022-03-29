// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage.journal.index;

/**
 * Journal index.
 */
public interface JournalIndex {

  /**
   * Adds an entry for the given index at the given position.
   *
   * @param index the index for which to add the entry
   * @param position the position of the given index
   */
  void index(long index, int position);

  /**
   * Looks up the position of the given index.
   *
   * @param index the index to lookup
   * @return the position of the given index or a lesser index
   */
  Position lookup(long index);

  /**
   * Truncates the index to the given index.
   *
   * @param index the index to which to truncate the index
   */
  void truncate(long index);

}
