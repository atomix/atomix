// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage.journal.index;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Journal index position.
 */
public class Position {
  private final long index;
  private final int position;

  public Position(long index, int position) {
    this.index = index;
    this.position = position;
  }

  public long index() {
    return index;
  }

  public int position() {
    return position;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("index", index)
        .add("position", position)
        .toString();
  }
}
