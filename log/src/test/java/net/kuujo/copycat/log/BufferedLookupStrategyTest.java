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
package net.kuujo.copycat.log;

import net.kuujo.copycat.log.compaction.BufferedLookupStrategy;
import net.kuujo.copycat.log.compaction.LookupStrategy;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

/**
 * Buffered lookup strategy test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class BufferedLookupStrategyTest {

  /**
   * Tests putting and getting an index in the lookup strategy.
   */
  public void testPutGetIndex() {
    LookupStrategy lookup = new BufferedLookupStrategy(100);
    Long result;
    lookup.put(ByteBuffer.wrap("Hello world!".getBytes()), 10L);
    result = lookup.get(ByteBuffer.wrap("Hello world!".getBytes()));
    Assert.assertEquals(result, Long.valueOf(10));
    result = lookup.get(ByteBuffer.wrap("Hello world again!".getBytes()));
    Assert.assertNull(result);
  }

}
