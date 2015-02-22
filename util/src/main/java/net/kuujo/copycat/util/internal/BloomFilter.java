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

import java.util.BitSet;
import java.util.Objects;

/**
 * Simple Murmur based Bloom Filter implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BloomFilter<T> {
  private final int numHashes;
  private final int numBits;
  private final BitSet bits;

  /**
   * Calculation of number of bits and hashes taken from Guava.
   *
   * https://github.com/google/guava/blob/master/guava/src/com/google/common/hash/BloomFilter.java
   *
   * @param falsePositiveProbability The desired probability of false positives.
   * @param expectedSize The expected number of elements to be added to the filter. This will be used in conjunction
   *                     with the desired false positive probability to calculate the number of bits and hashes to use.
   */
  public BloomFilter(double falsePositiveProbability, int expectedSize) {
    numBits = (int) (-expectedSize * Math.log(falsePositiveProbability == 0 ? Double.MIN_VALUE : falsePositiveProbability) / (Math.log(2) * Math.log(2)));
    numHashes = Math.max(1, (int) Math.round((double) numBits / expectedSize * Math.log(2)));
    bits = new BitSet(numBits);
  }

  /**
   * Returns an array of indexes of bits for the given byte array.
   *
   * @param bytes The byte array to index.
   * @param numHashes The number of hash functions to run and indexes to return.
   * @param bits The total number of available bits.
   * @return An array of bit indexes.
   */
  private static int[] indexes(byte[] bytes, int numHashes, int bits) {
    if (numHashes == 1) {
      return new int[]{Hash.hash32(bytes)};
    } else if (numHashes == 2) {
      return new int[]{Hash.hash32(bytes, 0), Hash.hash32(bytes, 1)};
    }

    int[] hashes = new int[numHashes];

    int h1 = Hash.hash32(bytes, 0);
    hashes[0] = Math.abs(h1 % bits);

    int h2 = Hash.hash32(bytes, 1);
    hashes[1] = Math.abs(h2 % bits);

    for (int i = 2; i < numHashes; i++) {
      int h = h1 + i * h2;
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
    for (int index : indexes(bytes, numHashes, numBits)) {
      if (bits.get(index)) {
        bits.set(index, true);
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
    for (int index : indexes(bytes, numHashes, numBits)) {
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
  public int size() {
    return numBits;
  }

  /**
   * Combines the given bloom filter with this bloom filter.
   *
   * @param filter The filter to combine.
   * @return The combined filter.
   */
  public BloomFilter<T> combine(BloomFilter<T> filter) {
    Assert.arg(filter, filter != this, "cannot combine a bloom filter with itself");
    Assert.arg(filter, filter.numBits == numBits, "cannot combine a bloom filter with a different number of bits");
    Assert.arg(filter, filter.numHashes == numHashes, "cannot combine a bloom filter with a different number of hashes");
    bits.or(filter.bits);
    return this;
  }

  /**
   * Calculates the probability that a false positive will occur.
   */
  private double calculateFalsePositiveProbability() {
    return 1 - Math.pow((double) bits.size() / numBits, numHashes);
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
