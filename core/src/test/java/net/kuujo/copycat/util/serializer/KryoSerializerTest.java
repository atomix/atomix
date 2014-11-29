/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.util.serializer;

import net.kuujo.copycat.util.serializer.internal.KryoSerializer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Random;

/**
 * Kryo serializer test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class KryoSerializerTest {

  public static class TestType1 {
    private long foo;
    private String bar;
    private boolean baz;

    public TestType1() {
    }

    public TestType1(long foo, String bar, boolean baz) {
      this.foo = foo;
      this.bar = bar;
      this.baz = baz;
    }
  }

  public void testWriteReadObjects() {
    Random random = new Random();
    Serializer serializer = new KryoSerializer();
    for (int i = 0; i < 1000; i++) {
      TestType1 testInput = new TestType1(random.nextLong(), "Hello world!", random.nextInt(1) == 1);
      byte[] bytes = serializer.writeObject(testInput);
      TestType1 testOutput = serializer.readObject(bytes);
      Assert.assertEquals(testOutput.foo, testInput.foo);
      Assert.assertEquals(testOutput.bar, testInput.bar);
      Assert.assertEquals(testOutput.baz, testInput.baz);
    }
  }

}
