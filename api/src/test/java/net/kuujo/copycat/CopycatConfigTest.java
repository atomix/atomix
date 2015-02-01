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
package net.kuujo.copycat;

import net.kuujo.copycat.util.serializer.KryoSerializer;
import org.testng.annotations.Test;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Copycat configuration test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class CopycatConfigTest {

  /**
   * Test configuration setters and getters.
   */
  public void testSettersAndGetters() throws Throwable {
    CopycatConfig config = new CopycatConfig();
    assertEquals(config.getName(), "copycat");
    config.setName("foo");
    assertEquals(config.getName(), "foo");
    config.setDefaultSerializer(KryoSerializer.class);
    assertTrue(config.getDefaultSerializer() instanceof KryoSerializer);
    Executor executor = Executors.newSingleThreadExecutor();
    config.setDefaultExecutor(executor);
    assertEquals(config.getDefaultExecutor(), executor);
  }

}
