package io.atomix.messaging.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

/**
 * Message decoder test.
 */
public class MessageDecoderTest {

    @Test
    public void testReadStringFromHeapBuffer() throws Exception {
        String payload = "huuhaa";
        ByteBuf byteBuf = Unpooled.wrappedBuffer(payload.getBytes(StandardCharsets.UTF_8));
        try {
            assertEquals(payload, MessageDecoder.readString(byteBuf, payload.length(), StandardCharsets.UTF_8));
        } finally {
            byteBuf.release();
        }
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
        byteBuf = Unpooled.buffer(4 + bytes.length);
        try {
            byteBuf.writeInt(1);
            byteBuf.writeBytes(bytes);
            byteBuf.readInt();
            assertEquals(payload, MessageDecoder.readString(byteBuf, payload.length(), StandardCharsets.UTF_8));
        } finally {
            byteBuf.release();
        }
    }

    @Test
    public void testReadStringFromDirectBuffer() throws Exception {
        String payload = "huuhaa";
        ByteBuf byteBuf = Unpooled.directBuffer(payload.length()).writeBytes(payload.getBytes(StandardCharsets.UTF_8));
        try {
            assertEquals(payload, MessageDecoder.readString(byteBuf, payload.length(), StandardCharsets.UTF_8));
        } finally {
            byteBuf.release();
        }
    }
}
