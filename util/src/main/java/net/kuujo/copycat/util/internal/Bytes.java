package net.kuujo.copycat.util.internal;

import java.nio.ByteBuffer;

public final class Bytes {
  private Bytes() {
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
