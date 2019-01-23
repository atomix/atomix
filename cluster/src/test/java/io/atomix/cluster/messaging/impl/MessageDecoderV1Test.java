/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.cluster.messaging.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

/**
 * Message decoder test.
 */
public class MessageDecoderV1Test {
  @Test
  public void testDecodeCompactInt() throws Exception {
    ByteBuf buffer = Unpooled.buffer(5);
    MessageEncoderV1.writeInt(buffer, 10);
    assertEquals(10, MessageDecoderV1.readInt(buffer));

    buffer = Unpooled.buffer(2);
    MessageEncoderV1.writeInt(buffer, 10);
    assertEquals(10, MessageDecoderV1.readInt(buffer));
  }

  @Test
  public void testDecodeCompactLong() throws Exception {
    ByteBuf buffer = Unpooled.buffer(9);
    MessageEncoderV1.writeLong(buffer, 10);
    assertEquals(10, MessageDecoderV1.readLong(buffer));

    buffer = Unpooled.buffer(2);
    MessageEncoderV1.writeLong(buffer, 10);
    assertEquals(10, MessageDecoderV1.readLong(buffer));
  }

  @Test
  public void testReadStringFromHeapBuffer() throws Exception {
    String payload = "huuhaa";
    ByteBuf byteBuf = Unpooled.wrappedBuffer(payload.getBytes(StandardCharsets.UTF_8));
    try {
      assertEquals(payload, MessageDecoderV1.readString(byteBuf, payload.length()));
    } finally {
      byteBuf.release();
    }
    byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
    byteBuf = Unpooled.buffer(4 + bytes.length);
    try {
      byteBuf.writeInt(1);
      byteBuf.writeBytes(bytes);
      byteBuf.readInt();
      assertEquals(payload, MessageDecoderV1.readString(byteBuf, payload.length()));
    } finally {
      byteBuf.release();
    }
  }

  @Test
  public void testReadStringFromDirectBuffer() throws Exception {
    String payload = "huuhaa";
    ByteBuf byteBuf = Unpooled.directBuffer(payload.length()).writeBytes(payload.getBytes(StandardCharsets.UTF_8));
    try {
      assertEquals(payload, MessageDecoderV1.readString(byteBuf, payload.length()));
    } finally {
      byteBuf.release();
    }
  }
}
