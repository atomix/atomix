// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.serializer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BufferAwareByteArrayOutputStreamTest {

  @Test
  public void testBufferSize() throws Exception {
    BufferAwareByteArrayOutputStream outputStream = new BufferAwareByteArrayOutputStream(8);
    assertEquals(8, outputStream.getBufferSize());
    outputStream.write(new byte[]{1, 2, 3, 4, 5, 6, 7, 8});
    assertEquals(8, outputStream.getBufferSize());
    outputStream.write(new byte[]{1, 2, 3, 4, 5, 6, 7, 8});
    assertEquals(16, outputStream.getBufferSize());
    outputStream.reset();
    assertEquals(16, outputStream.getBufferSize());
  }
}
