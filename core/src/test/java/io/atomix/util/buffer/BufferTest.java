/*
 * Copyright 2015-present Open Networking Laboratory
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
package io.atomix.util.buffer;

import org.testng.annotations.Test;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteOrder;

import static org.testng.Assert.*;

/**
 * Base buffer test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public abstract class BufferTest {

  /**
   * Creates a new test buffer.
   */
  protected abstract Buffer createBuffer(long capacity);

  /**
   * Creates a new test buffer.
   */
  protected abstract Buffer createBuffer(long capacity, long maxCapacity);

  public void testPosition() {
    Buffer buffer = createBuffer(8);
    assertEquals(buffer.position(), 0);
    buffer.writeInt(10);
    assertEquals(buffer.position(), 4);
    buffer.position(0);
    assertEquals(buffer.position(), 0);
    assertEquals(buffer.readInt(), 10);
  }

  public void testFlip() {
    Buffer buffer = createBuffer(8);
    buffer.writeInt(10);
    assertEquals(buffer.position(), 4);
    assertEquals(buffer.capacity(), 8);
    assertEquals(buffer.limit(), -1);
    assertEquals(buffer.capacity(), 8);
    buffer.flip();
    assertEquals(buffer.limit(), 4);
    assertEquals(buffer.position(), 0);
  }

  public void testLimit() {
    Buffer buffer = createBuffer(8);
    assertEquals(buffer.position(), 0);
    assertEquals(buffer.limit(), -1);
    assertEquals(buffer.capacity(), 8);
    buffer.limit(4);
    assertEquals(buffer.limit(), 4);
    assertTrue(buffer.hasRemaining());
    buffer.writeInt(10);
    assertEquals(buffer.remaining(), 0);
    assertFalse(buffer.hasRemaining());
  }

  public void testClear() {
    Buffer buffer = createBuffer(8);
    buffer.limit(6);
    assertEquals(buffer.limit(), 6);
    buffer.writeInt(10);
    assertEquals(buffer.position(), 4);
    buffer.clear();
    assertEquals(buffer.limit(), -1);
    assertEquals(buffer.capacity(), 8);
    assertEquals(buffer.position(), 0);
  }

  public void testMarkReset() {
    assertTrue(createBuffer(12).writeInt(10).mark().writeBoolean(true).reset().readBoolean());
  }

  @Test(expectedExceptions=BufferUnderflowException.class)
  public void testReadIntThrowsBufferUnderflowWithNoRemainingBytesRelative() {
    createBuffer(4, 4)
      .writeInt(10)
      .readInt();
  }

  @Test(expectedExceptions=BufferUnderflowException.class)
  public void testReadIntThrowsBufferUnderflowWithNoRemainingBytesAbsolute() {
    createBuffer(4, 4).readInt(2);
  }

  @Test(expectedExceptions=BufferOverflowException.class)
  public void testWriteIntThrowsBufferOverflowWithNoRemainingBytesRelative() {
    createBuffer(4, 4).writeInt(10).writeInt(20);
  }

  @Test(expectedExceptions=BufferOverflowException.class)
  public void testReadIntThrowsBufferOverflowWithNoRemainingBytesAbsolute() {
    createBuffer(4, 4).writeInt(4, 10);
  }

  @Test(expectedExceptions=IndexOutOfBoundsException.class)
  public void testReadIntThrowsIndexOutOfBounds() {
    createBuffer(4, 4).readInt(10);
  }

  @Test(expectedExceptions=IndexOutOfBoundsException.class)
  public void testWriteIntThrowsIndexOutOfBounds() {
    createBuffer(4, 4).writeInt(10, 10);
  }

  public void testWriteReadByteRelative() {
    assertEquals(createBuffer(16).writeByte(10).flip().readByte(), 10);
  }

  public void testWriteReadByteAbsolute() {
    assertEquals(createBuffer(16).writeByte(4, 10).readByte(4), 10);
  }

  public void testWriteReadUnsignedByteRelative() {
    assertEquals(createBuffer(16).writeUnsignedByte(10).flip().readUnsignedByte(), 10);
  }

  public void testWriteReadUnsignedByteAbsolute() {
    assertEquals(createBuffer(16).writeUnsignedByte(4, 10).readUnsignedByte(4), 10);
  }

  public void testWriteReadShortRelative() {
    assertEquals(createBuffer(16).writeShort((short) 10).flip().readShort(), 10);
  }

  public void testWriteReadShortAbsolute() {
    assertEquals(createBuffer(16).writeShort(4, (short) 10).readShort(4), 10);
  }

  public void testWriteReadUnsignedShortRelative() {
    assertEquals(createBuffer(16).writeUnsignedShort((short) 10).flip().readUnsignedShort(), 10);
  }

  public void testWriteReadUnsignedShortAbsolute() {
    assertEquals(createBuffer(16).writeUnsignedShort(4, (short) 10).readUnsignedShort(4), 10);
  }

  public void testWriteReadIntRelative() {
    assertEquals(createBuffer(16).writeInt(10).flip().readInt(), 10);
  }

  public void testWriteReadUnsignedIntAbsolute() {
    assertEquals(createBuffer(16).writeUnsignedInt(4, 10).readUnsignedInt(4), 10);
  }

  public void testWriteReadUnsignedIntRelative() {
    assertEquals(createBuffer(16).writeUnsignedInt(10).flip().readUnsignedInt(), 10);
  }

  public void testWriteReadIntAbsolute() {
    assertEquals(createBuffer(16).writeInt(4, 10).readInt(4), 10);
  }

  public void testWriteReadLongRelative() {
    assertEquals(createBuffer(16).writeLong(12345).flip().readLong(), 12345);
  }

  public void testWriteReadLongAbsolute() {
    assertEquals(createBuffer(16).writeLong(4, 12345).readLong(4), 12345);
  }

  public void testWriteReadFloatRelative() {
    assertEquals(createBuffer(16).writeFloat(10.6f).flip().readFloat(), 10.6f);
  }

  public void testWriteReadFloatAbsolute() {
    assertEquals(createBuffer(16).writeFloat(4, 10.6f).readFloat(4), 10.6f);
  }

  public void testWriteReadDoubleRelative() {
    assertEquals(createBuffer(16).writeDouble(10.6).flip().readDouble(), 10.6);
  }

  public void testWriteReadDoubleAbsolute() {
    assertEquals(createBuffer(16).writeDouble(4, 10.6).readDouble(4), 10.6);
  }

  public void testWriteReadBooleanRelative() {
    assertTrue(createBuffer(16).writeBoolean(true).flip().readBoolean());
  }

  public void testWriteReadBooleanAbsolute() {
    assertTrue(createBuffer(16).writeBoolean(4, true).readBoolean(4));
  }

  public void testWriteReadStringRelative() {
    Buffer buffer = createBuffer(38)
        .writeString("Hello world!")
        .writeString("Hello world again!")
        .flip();
    assertEquals(buffer.readString(), "Hello world!");
    assertEquals(buffer.readString(), "Hello world again!");
  }

  public void testWriteReadStringAbsolute() {
    Buffer buffer = createBuffer(46)
        .writeString(4, "Hello world!")
        .writeString(20, "Hello world again!");
    assertEquals(buffer.readString(4), "Hello world!");
    assertEquals(buffer.readString(20), "Hello world again!");
  }

  public void testWriteReadUTF8Relative() {
    Buffer buffer = createBuffer(38)
        .writeUTF8("Hello world!")
        .writeUTF8("Hello world again!")
        .flip();
    assertEquals(buffer.readUTF8(), "Hello world!");
    assertEquals(buffer.readUTF8(), "Hello world again!");
  }

  public void testWriteReadUTF8Absolute() {
    Buffer buffer = createBuffer(46)
        .writeUTF8(4, "Hello world!")
        .writeUTF8(20, "Hello world again!");
    assertEquals(buffer.readUTF8(4), "Hello world!");
    assertEquals(buffer.readUTF8(20), "Hello world again!");
  }

  public void testReadWriter() {
    Buffer writeBuffer = createBuffer(8).writeLong(10).flip();
    Buffer readBuffer = createBuffer(8);
    writeBuffer.read(readBuffer);
    assertEquals(readBuffer.flip().readLong(), 10);
  }

  public void testWriteReadSwappedIntRelative() {
    assertEquals(createBuffer(16).order(ByteOrder.LITTLE_ENDIAN).writeInt(10).flip().readInt(), 10);
  }

  public void testWriteReadSwappedIntAbsolute() {
    assertEquals(createBuffer(16).order(ByteOrder.LITTLE_ENDIAN).writeInt(4, 10).readInt(4), 10);
  }

  public void testAbsoluteSlice() {
    Buffer buffer = createBuffer(1024);
    buffer.writeLong(10).writeLong(11).rewind();
    Buffer slice = buffer.slice(8, 1016);
    assertEquals(slice.position(), 0);
    assertEquals(slice.readLong(), 11);
  }

  public void testRelativeSliceWithoutLength() {
    Buffer buffer = createBuffer(1024, 1024);
    buffer.writeLong(10).writeLong(11).writeLong(12).rewind();
    assertEquals(buffer.readLong(), 10);
    Buffer slice = buffer.slice();
    assertEquals(slice.position(), 0);
    assertEquals(slice.limit(), -1);
    assertEquals(slice.capacity(), 1016);
    assertEquals(slice.maxCapacity(), 1016);
    assertEquals(slice.readLong(), 11);
    assertEquals(slice.readLong(0), 11);
    slice.close();
    Buffer slice2 = buffer.skip(8).slice();
    assertEquals(slice2.position(), 0);
    assertEquals(slice2.limit(), -1);
    assertEquals(slice2.capacity(), 1008);
    assertEquals(slice2.maxCapacity(), 1008);
    assertEquals(slice2.readLong(), 12);
    assertEquals(slice2.readLong(0), 12);
  }

  public void testRelativeSliceWithLength() {
    Buffer buffer = createBuffer(1024);
    buffer.writeLong(10).writeLong(11).writeLong(12).rewind();
    assertEquals(buffer.readLong(), 10);
    Buffer slice = buffer.slice(8);
    assertEquals(slice.position(), 0);
    assertEquals(slice.readLong(), 11);
    assertEquals(slice.readLong(0), 11);
    slice.close();
    Buffer slice2 = buffer.skip(8).slice(8);
    assertEquals(slice2.position(), 0);
    assertEquals(slice2.readLong(), 12);
    assertEquals(slice2.readLong(0), 12);
    slice2.close();
  }

  public void testSliceOfSlice() {
    Buffer buffer = createBuffer(1024);
    buffer.writeLong(10).writeLong(11).writeLong(12).rewind();
    assertEquals(buffer.readLong(), 10);
    Buffer slice = buffer.slice();
    assertEquals(slice.readLong(), 11);
    Buffer sliceOfSlice = slice.slice();
    assertEquals(sliceOfSlice.readLong(), 12);
    assertEquals(sliceOfSlice.position(), 8);
  }

  public void testSliceWithLimit() {
    Buffer buffer = createBuffer(1024).limit(16);
    buffer.writeLong(10);
    Buffer slice = buffer.slice();
    assertEquals(slice.position(), 0);
    assertEquals(slice.capacity(), 8);
    assertEquals(slice.maxCapacity(), 8);
    assertEquals(slice.remaining(), 8);
  }

  public void testSliceWithLittleRemaining() {
    Buffer buffer = createBuffer(1024, 2048);
    buffer.position(1020);
    Buffer slice = buffer.slice(8);
    assertEquals(slice.position(), 0);
    assertEquals(slice.limit(), -1);
  }

  public void testCompact() {
    Buffer buffer = createBuffer(1024);
    buffer.position(100).writeLong(1234).position(100).compact();
    assertEquals(buffer.position(), 0);
    assertEquals(buffer.readLong(), 1234);
  }

  public void testSwappedPosition() {
    Buffer buffer = createBuffer(8).order(ByteOrder.LITTLE_ENDIAN);
    assertEquals(buffer.position(), 0);
    buffer.writeInt(10);
    assertEquals(buffer.position(), 4);
    buffer.position(0);
    assertEquals(buffer.position(), 0);
    assertEquals(buffer.readInt(), 10);
  }

  public void testSwappedFlip() {
    Buffer buffer = createBuffer(8).order(ByteOrder.LITTLE_ENDIAN);
    buffer.writeInt(10);
    assertEquals(buffer.position(), 4);
    assertEquals(buffer.capacity(), 8);
    assertEquals(buffer.limit(), -1);
    assertEquals(buffer.capacity(), 8);
    buffer.flip();
    assertEquals(buffer.limit(), 4);
    assertEquals(buffer.position(), 0);
  }

  public void testSwappedLimit() {
    Buffer buffer = createBuffer(8).order(ByteOrder.LITTLE_ENDIAN);
    assertEquals(buffer.position(), 0);
    assertEquals(buffer.limit(), -1);
    assertEquals(buffer.capacity(), 8);
    buffer.limit(4);
    assertEquals(buffer.limit(), 4);
    assertTrue(buffer.hasRemaining());
    buffer.writeInt(10);
    assertEquals(buffer.remaining(), 0);
    assertFalse(buffer.hasRemaining());
  }

  public void testSwappedClear() {
    Buffer buffer = createBuffer(8).order(ByteOrder.LITTLE_ENDIAN);
    buffer.limit(6);
    assertEquals(buffer.limit(), 6);
    buffer.writeInt(10);
    assertEquals(buffer.position(), 4);
    buffer.clear();
    assertEquals(buffer.limit(), -1);
    assertEquals(buffer.capacity(), 8);
    assertEquals(buffer.position(), 0);
  }

  public void testSwappedMarkReset() {
    assertTrue(createBuffer(12).order(ByteOrder.LITTLE_ENDIAN).writeInt(10).mark().writeBoolean(true).reset().readBoolean());
  }

  @Test(expectedExceptions=BufferUnderflowException.class)
  public void testSwappedReadIntThrowsBufferUnderflowWithNoRemainingBytesRelative() {
    createBuffer(4, 4).order(ByteOrder.LITTLE_ENDIAN)
      .writeInt(10)
      .readInt();
  }

  @Test(expectedExceptions=BufferUnderflowException.class)
  public void testSwappedReadIntThrowsBufferUnderflowWithNoRemainingBytesAbsolute() {
    createBuffer(4, 4).order(ByteOrder.LITTLE_ENDIAN).readInt(2);
  }

  @Test(expectedExceptions=BufferOverflowException.class)
  public void testSwappedWriteIntThrowsBufferOverflowWithNoRemainingBytesRelative() {
    createBuffer(4, 4).order(ByteOrder.LITTLE_ENDIAN).writeInt(10).writeInt(20);
  }

  @Test(expectedExceptions=BufferOverflowException.class)
  public void testSwappedReadIntThrowsBufferOverflowWithNoRemainingBytesAbsolute() {
    createBuffer(4, 4).order(ByteOrder.LITTLE_ENDIAN).writeInt(4, 10);
  }

  @Test(expectedExceptions=IndexOutOfBoundsException.class)
  public void testSwappedReadIntThrowsIndexOutOfBounds() {
    createBuffer(4, 4).order(ByteOrder.LITTLE_ENDIAN).readInt(10);
  }

  @Test(expectedExceptions=IndexOutOfBoundsException.class)
  public void testSwappedWriteIntThrowsIndexOutOfBounds() {
    createBuffer(4, 4).order(ByteOrder.LITTLE_ENDIAN).writeInt(10, 10);
  }

  public void testSwappedWriteReadByteRelative() {
    assertEquals(createBuffer(16).order(ByteOrder.LITTLE_ENDIAN).writeByte(10).flip().readByte(), 10);
  }

  public void testSwappedWriteReadByteAbsolute() {
    assertEquals(createBuffer(16).order(ByteOrder.LITTLE_ENDIAN).writeByte(4, 10).readByte(4), 10);
  }

  public void testSwappedWriteReadUnsignedByteRelative() {
    assertEquals(createBuffer(16).order(ByteOrder.LITTLE_ENDIAN).writeUnsignedByte(10).flip().readUnsignedByte(), 10);
  }

  public void testSwappedWriteReadUnsignedByteAbsolute() {
    assertEquals(createBuffer(16).order(ByteOrder.LITTLE_ENDIAN).writeUnsignedByte(4, 10).readUnsignedByte(4), 10);
  }

  public void testSwappedWriteReadShortRelative() {
    assertEquals(createBuffer(16).order(ByteOrder.LITTLE_ENDIAN).writeShort((short) 10).flip().readShort(), 10);
  }

  public void testSwappedWriteReadShortAbsolute() {
    assertEquals(createBuffer(16).order(ByteOrder.LITTLE_ENDIAN).writeShort(4, (short) 10).readShort(4), 10);
  }

  public void testSwappedWriteReadUnsignedShortRelative() {
    assertEquals(createBuffer(16).order(ByteOrder.LITTLE_ENDIAN).writeUnsignedShort((short) 10).flip().readUnsignedShort(), 10);
  }

  public void testSwappedWriteReadUnsignedShortAbsolute() {
    assertEquals(createBuffer(16).order(ByteOrder.LITTLE_ENDIAN).writeUnsignedShort(4, (short) 10).readUnsignedShort(4), 10);
  }

  public void testSwappedWriteReadIntRelative() {
    assertEquals(createBuffer(16).order(ByteOrder.LITTLE_ENDIAN).writeInt(10).flip().readInt(), 10);
  }

  public void testSwappedWriteReadUnsignedIntAbsolute() {
    assertEquals(createBuffer(16).order(ByteOrder.LITTLE_ENDIAN).writeUnsignedInt(4, 10).readUnsignedInt(4), 10);
  }

  public void testSwappedWriteReadUnsignedIntRelative() {
    assertEquals(createBuffer(16).order(ByteOrder.LITTLE_ENDIAN).writeUnsignedInt(10).flip().readUnsignedInt(), 10);
  }

  public void testSwappedWriteReadIntAbsolute() {
    assertEquals(createBuffer(16).order(ByteOrder.LITTLE_ENDIAN).writeInt(4, 10).readInt(4), 10);
  }

  public void testSwappedWriteReadLongRelative() {
    assertEquals(createBuffer(16).order(ByteOrder.LITTLE_ENDIAN).writeLong(12345).flip().readLong(), 12345);
  }

  public void testSwappedWriteReadLongAbsolute() {
    assertEquals(createBuffer(16).order(ByteOrder.LITTLE_ENDIAN).writeLong(4, 12345).readLong(4), 12345);
  }

  public void testSwappedWriteReadFloatRelative() {
    assertEquals(createBuffer(16).order(ByteOrder.LITTLE_ENDIAN).writeFloat(10.6f).flip().readFloat(), 10.6f);
  }

  public void testSwappedWriteReadFloatAbsolute() {
    assertEquals(createBuffer(16).order(ByteOrder.LITTLE_ENDIAN).writeFloat(4, 10.6f).readFloat(4), 10.6f);
  }

  public void testSwappedWriteReadDoubleRelative() {
    assertEquals(createBuffer(16).order(ByteOrder.LITTLE_ENDIAN).writeDouble(10.6).flip().readDouble(), 10.6);
  }

  public void testSwappedWriteReadDoubleAbsolute() {
    assertEquals(createBuffer(16).order(ByteOrder.LITTLE_ENDIAN).writeDouble(4, 10.6).readDouble(4), 10.6);
  }

  public void testSwappedWriteReadBooleanRelative() {
    assertTrue(createBuffer(16).order(ByteOrder.LITTLE_ENDIAN).writeBoolean(true).flip().readBoolean());
  }

  public void testSwappedWriteReadBooleanAbsolute() {
    assertTrue(createBuffer(16).order(ByteOrder.LITTLE_ENDIAN).writeBoolean(4, true).readBoolean(4));
  }

  public void testSwappedReadWriter() {
    Buffer writeBuffer = createBuffer(8).order(ByteOrder.LITTLE_ENDIAN).writeLong(10).flip();
    Buffer readBuffer = createBuffer(8).order(ByteOrder.LITTLE_ENDIAN);
    writeBuffer.read(readBuffer);
    assertEquals(readBuffer.flip().readLong(), 10);
  }

  public void testSwappedAbsoluteSlice() {
    Buffer buffer = createBuffer(1024).order(ByteOrder.LITTLE_ENDIAN);
    buffer.writeLong(10).writeLong(11).rewind();
    Buffer slice = buffer.slice(8, 1016);
    assertEquals(slice.position(), 0);
    assertEquals(slice.readLong(), 11);
  }

  public void testSwappedRelativeSliceWithoutLength() {
    Buffer buffer = createBuffer(1024, 1024).order(ByteOrder.LITTLE_ENDIAN);
    buffer.writeLong(10).writeLong(11).writeLong(12).rewind();
    assertEquals(buffer.readLong(), 10);
    Buffer slice = buffer.slice();
    assertEquals(slice.position(), 0);
    assertEquals(slice.limit(), -1);
    assertEquals(slice.capacity(), 1016);
    assertEquals(slice.maxCapacity(), 1016);
    assertEquals(slice.readLong(), 11);
    assertEquals(slice.readLong(0), 11);
    slice.close();
    Buffer slice2 = buffer.skip(8).slice();
    assertEquals(slice2.position(), 0);
    assertEquals(slice2.limit(), -1);
    assertEquals(slice2.capacity(), 1008);
    assertEquals(slice2.maxCapacity(), 1008);
    assertEquals(slice2.readLong(), 12);
    assertEquals(slice2.readLong(0), 12);
  }

  public void testSwappedRelativeSliceWithLength() {
    Buffer buffer = createBuffer(1024).order(ByteOrder.LITTLE_ENDIAN);
    buffer.writeLong(10).writeLong(11).writeLong(12).rewind();
    assertEquals(buffer.readLong(), 10);
    Buffer slice = buffer.slice(8);
    assertEquals(slice.position(), 0);
    assertEquals(slice.readLong(), 11);
    assertEquals(slice.readLong(0), 11);
    slice.close();
    Buffer slice2 = buffer.skip(8).slice(8);
    assertEquals(slice2.position(), 0);
    assertEquals(slice2.readLong(), 12);
    assertEquals(slice2.readLong(0), 12);
    slice2.close();
  }

  public void testSwappedSliceOfSlice() {
    Buffer buffer = createBuffer(1024).order(ByteOrder.LITTLE_ENDIAN);
    buffer.writeLong(10).writeLong(11).writeLong(12).rewind();
    assertEquals(buffer.readLong(), 10);
    Buffer slice = buffer.slice();
    assertEquals(slice.readLong(), 11);
    Buffer sliceOfSlice = slice.slice();
    assertEquals(sliceOfSlice.readLong(), 12);
    assertEquals(sliceOfSlice.position(), 8);
  }

  public void testSwappedSliceWithLimit() {
    Buffer buffer = createBuffer(1024).order(ByteOrder.LITTLE_ENDIAN).limit(16);
    buffer.writeLong(10);
    Buffer slice = buffer.slice();
    assertEquals(slice.position(), 0);
    assertEquals(slice.capacity(), 8);
    assertEquals(slice.maxCapacity(), 8);
    assertEquals(slice.remaining(), 8);
  }

  public void testSwappedSliceWithLittleRemaining() {
    Buffer buffer = createBuffer(1024, 2048).order(ByteOrder.LITTLE_ENDIAN);
    buffer.position(1020);
    Buffer slice = buffer.slice(8);
    assertEquals(slice.position(), 0);
    assertEquals(slice.limit(), -1);
  }

  public void testSwappedCompact() {
    Buffer buffer = createBuffer(1024).order(ByteOrder.LITTLE_ENDIAN);
    buffer.position(100).writeLong(1234).position(100).compact();
    assertEquals(buffer.position(), 0);
    assertEquals(buffer.readLong(), 1234);
  }

  public void testCapacity0Read() {
    Buffer buffer = createBuffer(0, 1024);
    assertEquals(buffer.readLong(), 0);
  }

  public void testCapacity0Write() {
    Buffer buffer = createBuffer(0, 1024);
    buffer.writeLong(10);
    assertEquals(buffer.readLong(0), 10);
  }

}
