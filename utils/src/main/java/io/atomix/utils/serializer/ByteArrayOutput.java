// SPDX-FileCopyrightText: 2014-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.serializer;

import com.esotericsoftware.kryo.io.Output;

/**
 * Convenience class to avoid extra object allocation and casting.
 */
final class ByteArrayOutput extends Output {

  private final BufferAwareByteArrayOutputStream stream;

  ByteArrayOutput(final int bufferSize, final int maxBufferSize, final BufferAwareByteArrayOutputStream stream) {
    super(bufferSize, maxBufferSize);
    super.setOutputStream(stream);
    this.stream = stream;
  }

  BufferAwareByteArrayOutputStream getByteArrayOutputStream() {
    return stream;
  }
}
