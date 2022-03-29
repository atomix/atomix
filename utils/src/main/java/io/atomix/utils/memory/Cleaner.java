// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.memory;

import java.io.IOException;
import java.nio.ByteBuffer;

@FunctionalInterface
interface Cleaner {

  /**
   * Free {@link ByteBuffer} if possible.
   */
  void freeBuffer(ByteBuffer buffer) throws IOException;
}
