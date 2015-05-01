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
package net.kuujo.copycat.protocol.raft.storage;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.util.HashFunction;
import net.kuujo.copycat.io.util.Murmur3HashFunction;

import java.io.IOException;

/**
 * Segment key set.
 * <p>
 * The key set tracks the total number of unique keys within a given segment using a memory efficient estimator algorithm.
 * Specifically, this class uses AddThis's HyperLogLog++ implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class KeySet {
  private final HashFunction hash = new Murmur3HashFunction();
  private final ICardinality cardinality;

  KeySet() {
    cardinality = new HyperLogLogPlus(10, 14);
  }

  public KeySet(byte[] bytes) {
    try {
      cardinality = HyperLogLogPlus.Builder.build(bytes);
    } catch (IOException e) {
      throw new StorageException("failed to deserialize cardinality estimator");
    }
  }

  private KeySet(ICardinality cardinality) {
    this.cardinality = cardinality;
  }

  /**
   * Returns the cardinality of the set.
   *
   * @return The cardinality of the set.
   */
  public long cardinality() {
    return cardinality.cardinality();
  }

  /**
   * Adds a key to the set.
   *
   * @param key The key to add.
   */
  public void add(Buffer key) {
    cardinality.offerHashed(hash.hash64(key));
  }

  /**
   * Merges two key sets together.
   *
   * @param keySet The key set to merge in to this key set.
   * @return The merged key set.
   */
  public KeySet merge(KeySet keySet) {
    try {
      return new KeySet(cardinality.merge(keySet.cardinality));
    } catch (CardinalityMergeException e) {
      throw new StorageException("failed to merge cardinality estimators", e);
    }
  }

  /**
   * Serializes the key set.
   *
   * @return The serialized key set.
   */
  public byte[] getBytes() {
    try {
      return cardinality.getBytes();
    } catch (IOException e) {
      throw new StorageException("failed to serialize cardinality estimator", e);
    }
  }

  @Override
  public String toString() {
    return String.format("KeySet[cardinality=%d]", cardinality.cardinality());
  }

}
