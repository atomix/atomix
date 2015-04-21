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

/**
 * Hash function constants.
 * <p>
 * This basic enum provides singleton instances of hash functions. The hash functions provided by these constants have
 * a seed value of {@code 0}. If you need to build a hash function with a different seed value then you must construct
 * a {@link net.kuujo.copycat.io.util.HashFunction} instance directly.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public enum HashFunctions implements HashFunction {

  /**
   * City hash function.
   */
  CITYHASH {
    private final HashFunction function = new CityHashFunction();
    @Override
    public long hash64(byte[] bytes, int offset, int length) {
      return function.hash64(bytes, offset, length);
    }
  },

  /**
   * 64-bit Murmur3 hash function.
   */
  MURMUR3 {
    private final HashFunction function = new Murmur3HashFunction();
    @Override
    public long hash64(byte[] bytes, int offset, int length) {
      return function.hash64(bytes, offset, length);
    }
  }

}
