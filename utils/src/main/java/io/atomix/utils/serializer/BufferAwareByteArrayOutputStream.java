// SPDX-FileCopyrightText: 2014-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.serializer;

import java.io.ByteArrayOutputStream;

/**
 * Exposes protected byte array length in {@link ByteArrayOutputStream}.
 */
final class BufferAwareByteArrayOutputStream extends ByteArrayOutputStream {

  BufferAwareByteArrayOutputStream(int size) {
    super(size);
  }

  int getBufferSize() {
    return buf.length;
  }
}
