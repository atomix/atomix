/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.util.internal;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Hash utility.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class Hash {
  private static final int C1 = 0xcc9e2d51;
  private static final int C2 = 0x1b873593;

  /**
   * Hashes the given string value.
   *
   * Taken from Guava:
   * https://github.com/google/guava/blob/master/guava/src/com/google/common/hash/Murmur3_32HashFunction.java
   *
   * Authors:
   * - Austin Appleby
   * - Dimitris Andreou
   * - Kurt Alfred Kluever
   */
  public static int hash32(byte[] bytes) {
    int h1 = 0;
    int length = 0;

    ByteBuffer buffer = ByteBuffer.allocate(11).order(ByteOrder.LITTLE_ENDIAN);
    ByteBuffer readBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

    if (readBuffer.remaining() <= buffer.remaining()) {
      buffer.put(readBuffer);
      if (buffer.remaining() < 8) {
        // Munch
        buffer.flip();
        while (buffer.remaining() >= 4) {
          // Process bytes
          int k1 = mixK1(buffer.getInt());
          h1 = mixH1(h1, k1);
          length += 4;
        }
        buffer.compact();
      }
    } else {
      int bytesToCopy = 4 - buffer.position();
      for (int i = 0; i < bytesToCopy; i++) {
        buffer.put(readBuffer.get());
      }

      // Munch
      buffer.flip();
      while (buffer.remaining() >= 4) {
        // Process bytes
        int k1 = mixK1(buffer.getInt());
        h1 = mixH1(h1, k1);
        length += 4;
      }
      buffer.compact();

      while (readBuffer.remaining() >= 4) {
        // Process bytes
        int k1 = mixK1(readBuffer.getInt());
        h1 = mixH1(h1, k1);
        length += 4;
      }
      buffer.put(readBuffer);
    }

    // Munch
    buffer.flip();
    while (buffer.remaining() >= 4) {
      // Process bytes
      int k1 = mixK1(buffer.getInt());
      h1 = mixH1(h1, k1);
      length += 4;
    }
    buffer.compact();

    buffer.flip();
    if (buffer.remaining() > 0) {
      // Process remaining bytes
      length += buffer.remaining();
      int k1 = 0;
      for (int i = 0; buffer.hasRemaining(); i += 8) {
        k1 ^= (buffer.get() & 0xFF) << i;
      }
      h1 ^= mixK1(k1);
    }

    return fmix(h1, length);
  }

  /**
   * Taken from Guava:
   * https://github.com/google/guava/blob/master/guava/src/com/google/common/hash/Murmur3_32HashFunction.java
   *
   * Authors:
   * - Austin Appleby
   * - Dimitris Andreou
   * - Kurt Alfred Kluever
   */
  private static int mixK1(int k1) {
    k1 *= C1;
    k1 = Integer.rotateLeft(k1, 15);
    k1 *= C2;
    return k1;
  }

  /**
   * Taken from Guava:
   * https://github.com/google/guava/blob/master/guava/src/com/google/common/hash/Murmur3_32HashFunction.java
   *
   * Authors:
   * - Austin Appleby
   * - Dimitris Andreou
   * - Kurt Alfred Kluever
   */
  private static int mixH1(int h1, int k1) {
    h1 ^= k1;
    h1 = Integer.rotateLeft(h1, 13);
    h1 = h1 * 5 + 0xe6546b64;
    return h1;
  }
  /**
   * Taken from Guava:
   * https://github.com/google/guava/blob/master/guava/src/com/google/common/hash/Murmur3_32HashFunction.java
   *
   * Authors:
   * - Austin Appleby
   * - Dimitris Andreou
   * - Kurt Alfred Kluever
   */
  private static int fmix(int h1, int length) {
    h1 ^= length;
    h1 ^= h1 >>> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >>> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >>> 16;
    return h1;
  }

}
