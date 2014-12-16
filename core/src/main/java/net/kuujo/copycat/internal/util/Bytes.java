package net.kuujo.copycat.internal.util;

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
}
