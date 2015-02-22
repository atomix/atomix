package net.kuujo.copycat.util.internal;

import java.nio.ByteBuffer;

public final class Bytes {
  private Bytes() {
  }

  /**
   * Gets the remaining bytes from the given byte buffer.
   *
   * @param buffer The buffer from which to read bytes.
   * @return The remaining bytes in the buffer.
   */
  public static byte[] getBytes(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return bytes;
  }

  /**
   * Returns a ByteBuffer wrapping the bytes of the {@code string}.
   */
  public static ByteBuffer of(String string) {
    return ByteBuffer.wrap(string.getBytes());
  }

  /**
   * Returns a ByteBuffer wrapping the bytes of the {@code number}.
   */
  public static ByteBuffer of(int number) {
    return ByteBuffer.allocate(4).putInt(number);
  }

}
