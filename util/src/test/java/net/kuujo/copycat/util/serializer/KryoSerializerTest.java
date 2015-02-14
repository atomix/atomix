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
package net.kuujo.copycat.util.serializer;

import net.jodah.concurrentunit.Waiter;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * Kryo serializer test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class KryoSerializerTest {

  /**
   * Asserts that concurrent serialization works.
   */
  public void shouldSerializeConcurrently() throws Throwable {
    Serializer serializer = new KryoSerializer();
    Waiter waiter = new Waiter();
    CountDownLatch latch = new CountDownLatch(10);

    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      threads.add(new Thread(() -> {
        try {
          latch.countDown();
          latch.await();
          String expected = UUID.randomUUID().toString();
          ByteBuffer buffer = serializer.writeObject(expected);
          Thread.sleep(100);
          Object result = serializer.readObject(buffer);
          waiter.assertEquals(result, expected);
          waiter.resume();
        } catch (Exception ignore) {
        }
      }));
    }

    threads.forEach(t -> t.start());
    waiter.await(5000, 10);
  }
}
