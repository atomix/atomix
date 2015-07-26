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
import net.kuujo.copycat.Node;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Asynchronous map test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class DistributedMapTest extends ConcurrentTestCase {

  /**
   * Tests putting and getting a value.
   */
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
  }

  /**
   * Tests the map size.
   */
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
  }

  /**
   * Tests TTL.
   */
  public void testMapTtl() throws Throwable {
    List<Copycat> copycats = createCopycats(3);

    Copycat copycat = copycats.get(0);

    Node node = copycat.create("/test").get();
    DistributedMap<String, String> map = node.create(DistributedMap.class).get();

    expectResume();
    map.put("foo", "Hello world!", 1, TimeUnit.SECONDS).thenRun(this::resume);
    await();

    expectResume();
    map.get("foo").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await();

    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() < startTime + 1000);

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
  /*
    TestMemberRegistry registry = new TestMemberRegistry();

    List<Copycat> copycats = new ArrayList<>();

    expectResumes(nodes);

    for (int i = 1; i <= nodes; i++) {
      TestCluster.Builder builder = TestCluster.builder()
        .withMemberId(i)
        .withRegistry(registry);

      for (int j = 1; j <= nodes; j++) {
        builder.addMember(TestMember.builder()
          .withId(j)
          .withAddress(String.format("test-%d", j))
          .build());
      }

      Copycat copycat = CopycatServer.builder()
        .withCluster(builder.build())
        .withLog(Log.builder()
          .withStorageLevel(StorageLevel.MEMORY)
          .build())
        .build();

      copycat.open().thenRun(this::resume);

      copycats.add(copycat);
    }*/

    await();

    return Collections.EMPTY_LIST;
  }

}
