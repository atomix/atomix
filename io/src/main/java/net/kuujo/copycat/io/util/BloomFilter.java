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
package net.kuujo.copycat.io.util;

import java.io.IOException;
import java.util.Objects;

/**
 * Simple Bloom Filter implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BloomFilter<T> implements AutoCloseable {
  private final int numHashes;
  private final long numBits;
  private final DirectBitSet bits;
  private final HashFunction function1;
  private final HashFunction function2;
  private int size;

  /**
   * Calculation of number of bits and hashes taken from Guava.
   *
   * https://github.com/google/guava/blob/master/guava/src/com/google/common/hash/BloomFilter.java
   *
   * @param falsePositiveProbability The desired probability of false positives.
   * @param expectedSize The expected number of elements to be added to the filter. This will be used in conjunction
   *                     with the desired false positive probability to calculate the number of bits and hashes to use.
   */
  public BloomFilter(double falsePositiveProbability, long expectedSize) {
    numBits = roundBits((long) (-expectedSize * Math.log(falsePositiveProbability == 0 ? Double.MIN_VALUE : falsePositiveProbability) / (Math.log(2) * Math.log(2))));
    numHashes = Math.max(1, (int) Math.round((double) numBits / expectedSize * Math.log(2)));
    bits = DirectBitSet.allocate(numBits);
    function1 = new CityHashFunction();
    function2 = new Murmur3HashFunction();
  }

  /**
   * Rounds the number of bits to the nearest power of two.
   */
  private static long roundBits(long bits) {
    if ((bits & (bits - 1)) == 0)
      return bits;
    int i = 128;
    while (i < bits) {
      i *= 2;
      if (i <= 0) return 1L << 62;
    }
    return i;
  }

  /**
   * Returns an array of indexes of bits for the given byte array.
   *
   * @param bytes The byte array to index.
   * @param numHashes The number of hash functions to run and indexes to return.
   * @param bits The total number of available bits.
   * @return An array of bit indexes.
   */
  private long[] indexes(byte[] bytes, int numHashes, long bits) {
    if (numHashes == 1) {
      return new long[]{Math.abs(function1.hashBytes(bytes) % bits)};
    } else if (numHashes == 2) {
      return new long[]{Math.abs(function1.hashBytes(bytes) % bits), Math.abs(function2.hashBytes(bytes) % bits)};
    }

    long[] hashes = new long[numHashes];

    long h1 = function1.hashBytes(bytes);
    hashes[0] = Math.abs(h1 % bits);

    long h2 = function2.hashBytes(bytes);
    hashes[1] = Math.abs(h2 % bits);

    for (int i = 2; i < numHashes; i++) {
      long h = h1 + i * h2;
      if (h < 0)
        h = ~h;
      hashes[i] = Math.abs(h % bits);
    }
    return hashes;
  }

  /**
   * Adds a byte array value to the bloom filter.
   *
   * @param bytes The byte array to add.
   * @return Indicates whether the filter's bits changed.
   */
  public boolean add(byte[] bytes) {
    boolean changed = false;
    for (long index : indexes(bytes, numHashes, numBits)) {
      if (!bits.get(index)) {
        bits.set(index);
        size++;
        changed = true;
      }
    }
    return changed;
  }

  /**
   * Adds a value to the bloom filter.
   *
   * @param value The value to add.
   * @return Indicates whether the filter's bits changed.
   */
  public boolean add(T value) {
    return add(value.toString().getBytes());
  }

  /**
   * Returns a boolean value indicating whether the bloom filter *might* contain the given bytes.
   *
   * @param bytes The bytes to check.
   * @return Indicates whether the bloom filter *might* contain the given bytes.
   */
  public boolean contains(byte[] bytes) {
    for (long index : indexes(bytes, numHashes, numBits)) {
      if (!bits.get(index)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns a boolean value indicating whether the bloom filter might contain the given value.
   *
   * @param value The value to check.
   * @return Indicates whether the bloom filter *might* contain the given value.
   */
  public boolean contains(T value) {
    return contains(value.toString().getBytes());
  }

  /**
   * Returns the number of bits in the bloom filter.
   *
   * @return The number of bits in the bloom filter.
   */
  public long size() {
    return numBits;
  }

  /**
   * Calculates the probability that a false positive will occur.
   */
  private double calculateFalsePositiveProbability() {
    return 1 - Math.pow((double) size / numBits, numHashes);
  }

  @Override
  public void close() throws IOException {
    bits.close();
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof BloomFilter) {
      BloomFilter filter = (BloomFilter) object;
      return filter.numBits == numBits && filter.numHashes == numHashes && filter.bits.equals(bits);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(numHashes, numBits, bits);
  }

  @Override
  public String toString() {
    return String.format("BloomFilter[probability=%f]", calculateFalsePositiveProbability());
  }

}
