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

import net.openhft.hashing.LongHashFunction;

/**
 * Cityhash function based on OpenHFT's Zero-Allocation-Hashing:
 * https://github.com/OpenHFT/Zero-Allocation-Hashing
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CityHashFunction implements HashFunction {
  private final LongHashFunction function;

  public CityHashFunction() {
    function = LongHashFunction.city_1_1();
  }

  public CityHashFunction(long seed) {
    function = LongHashFunction.city_1_1(seed);
  }

  @Override
  public long hash64(byte[] bytes, int offset, int length) {
    return function.hashBytes(bytes, offset, length);
  }

}
