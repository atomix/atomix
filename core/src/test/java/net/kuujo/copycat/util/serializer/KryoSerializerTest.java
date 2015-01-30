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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import net.jodah.concurrentunit.Waiter;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.cluster.internal.MemberInfo;

import org.testng.annotations.Test;

/**
 * Kryo serializer test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class KryoSerializerTest {

  /**
   * Tests serializing a collection of member info.
   */
  public void testSerializeMemberInfo() {
    Serializer serializer = new KryoSerializer();
    Map<String, MemberInfo> info = new HashMap<>();
    info.put("local://test1", new MemberInfo("local://test1", Member.Type.ACTIVE, Member.State.ALIVE));
    info.put("local://test2", new MemberInfo("local://test2", Member.Type.ACTIVE, Member.State.ALIVE));
    info.put("local://test3", new MemberInfo("local://test3", Member.Type.PASSIVE, Member.State.SUSPICIOUS));
    ByteBuffer buffer = serializer.writeObject(new ArrayList<>(info.values()));
    List<MemberInfo> result = serializer.readObject(buffer);
    assertEquals(result.get(0).uri(), "local://test1");
    assertTrue(result.get(0).type() == Member.Type.ACTIVE);
    assertTrue(result.get(0).state() == Member.State.ALIVE);
    assertEquals(result.get(1).uri(), "local://test2");
    assertTrue(result.get(1).type() == Member.Type.ACTIVE);
    assertTrue(result.get(1).state() == Member.State.ALIVE);
    assertEquals(result.get(2).uri(), "local://test3");
    assertTrue(result.get(2).type() == Member.Type.PASSIVE);
    assertTrue(result.get(2).state() == Member.State.SUSPICIOUS);
  }

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
