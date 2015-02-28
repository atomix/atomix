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
package net.kuujo.copycat.util;

import net.kuujo.copycat.util.hash.DirectBitSet;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

/**
 * Direct memory bit set test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class BitSetTest {

  public void testPerformance() {
    DirectBitSet bits = new DirectBitSet(1024 * 1024);
    long start = System.nanoTime();
    for (int i = 0; i < 1024 * 1024; i++) {
      bits.set(i);
    }
    long end = System.nanoTime();
    System.out.println(TimeUnit.NANOSECONDS.toMicros(end - start));for(;;);
  }

  /**
   * Tests the bit set.
   */
  public void testBitSet() {
    DirectBitSet bits = new DirectBitSet(1024);
    bits.set(10);
    bits.set(20);
    Assert.assertTrue(bits.get(10));
    Assert.assertTrue(bits.get(20));
    Assert.assertFalse(bits.get(30));
  }

}
