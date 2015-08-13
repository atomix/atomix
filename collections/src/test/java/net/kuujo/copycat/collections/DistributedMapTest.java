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
package net.kuujo.copycat.collections;

import net.jodah.concurrentunit.ConcurrentTestCase;
import net.kuujo.copycat.Copycat;
import net.kuujo.copycat.CopycatServer;
import net.kuujo.copycat.Node;
import net.kuujo.copycat.io.storage.Log;
import net.kuujo.copycat.io.storage.StorageLevel;
import net.kuujo.copycat.io.transport.LocalServerRegistry;
import net.kuujo.copycat.io.transport.LocalTransport;
import net.kuujo.copycat.raft.Member;
import net.kuujo.copycat.raft.Members;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Distributed map test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class DistributedMapTest extends ConcurrentTestCase {

  /**
   * Tests putting and getting a value.
   */
  @SuppressWarnings("unchecked")
  public void testPutGetRemove() throws Throwable {
    List<Copycat> copycats = createCopycats(3);

    Copycat copycat = copycats.get(0);

    Node node = copycat.create("/test").get();
    DistributedMap<String, String> map = node.create(DistributedMap.class).get();

    expectResume();
    map.put("foo", "Hello world!").thenRun(this::resume);
    await();

    expectResume();
    map.get("foo").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await();

    expectResume();
    map.remove("foo").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await();

    expectResume();
    map.get("foo").thenAccept(result -> {
      threadAssertNull(result);
      resume();
    });
    await();

    copycats.forEach(c -> c.close().join());
  }

  /**
   * Tests the map count.
   */
  @SuppressWarnings("unchecked")
  public void testMapSize() throws Throwable {
    List<Copycat> copycats = createCopycats(3);

    Copycat copycat = copycats.get(0);

    Node node = copycat.create("/test").get();
    DistributedMap<String, String> map = node.create(DistributedMap.class).get();

    expectResume();
    map.size().thenAccept(size -> {
      threadAssertEquals(size, 0);
      resume();
    });
    await();

    expectResume();
    map.put("foo", "Hello world!").thenRun(this::resume);
    await();

    expectResume();
    map.size().thenAccept(size -> {
      threadAssertEquals(size, 1);
      resume();
    });
    await();

    expectResume();
    map.put("bar", "Hello world again!").thenRun(this::resume);
    await();

    expectResume();
    map.size().thenAccept(size -> {
      threadAssertEquals(size, 2);
      resume();
    });
    await();

    copycats.forEach(c -> c.close().join());
  }

  /**
   * Tests TTL.
   */
  @SuppressWarnings("unchecked")
  public void testMapTtl() throws Throwable {
    List<Copycat> copycats = createCopycats(3);

    Copycat copycat = copycats.get(0);

    Node node = copycat.create("/test").get();
    DistributedMap<String, String> map = node.create(DistributedMap.class).get();

    expectResume();
    map.put("foo", "Hello world!", Duration.ofSeconds(1)).thenRun(this::resume);
    await();

    expectResume();
    map.get("foo").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await();

    Thread.sleep(3000);

    expectResume();
    map.get("foo").thenAccept(result -> {
      threadAssertNull(result);
      resume();
    });
    await();

    expectResume();
    map.size().thenAccept(size -> {
      threadAssertEquals(size, 0);
      resume();
    });
    await();
  }

  /**
   * Creates a Copycat instance.
   */
  private List<Copycat> createCopycats(int nodes) throws Throwable {
    LocalServerRegistry registry = new LocalServerRegistry();

    List<Copycat> copycats = new ArrayList<>();

    expectResumes(nodes);

    Members.Builder builder = Members.builder();
    for (int i = 1; i <= nodes; i++) {
      builder.addMember(Member.builder()
        .withId(i)
        .withHost("localhost")
        .withPort(5000 + i)
        .build());
    }

    Members members = builder.build();

    for (int i = 1; i <= nodes; i++) {
      Copycat copycat = CopycatServer.builder()
        .withMemberId(i)
        .withMembers(members)
        .withTransport(LocalTransport.builder()
          .withRegistry(registry)
          .build())
        .withLog(Log.builder()
          .withStorageLevel(StorageLevel.MEMORY)
          .build())
        .build();

      copycat.open().thenRun(this::resume);

      copycats.add(copycat);
    }

    await();

    return copycats;
  }

}
