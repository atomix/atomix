// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage.journal;

import io.atomix.utils.misc.ArraySizeHashPrinter;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Test entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TestEntry {
  private final byte[] bytes;

  public TestEntry(int size) {
    this(new byte[size]);
  }

  public TestEntry(byte[] bytes) {
    this.bytes = bytes;
  }

  public byte[] bytes() {
    return bytes;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("bytes", ArraySizeHashPrinter.of(bytes))
        .toString();
  }
}
