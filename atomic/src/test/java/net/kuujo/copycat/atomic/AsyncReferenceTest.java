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
package net.kuujo.copycat.atomic;

import net.jodah.concurrentunit.ConcurrentTestCase;
import net.kuujo.copycat.Copycat;
import net.kuujo.copycat.CopycatServer;
import net.kuujo.copycat.Node;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.StorageLevel;
import net.kuujo.copycat.raft.Member;
import net.kuujo.copycat.raft.Members;
import net.kuujo.copycat.transport.LocalServerRegistry;
import net.kuujo.copycat.transport.LocalTransport;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Async reference test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class AsyncReferenceTest extends ConcurrentTestCase {

  /**
   * Tests setting and getting a value.
   */
  @SuppressWarnings("unchecked")
  public void testSetGet() throws Throwable {
    Servers servers = createCopycats(3, 2);

    Copycat copycat = servers.active.get(0);

    Node node = copycat.create("/test").get();
    AsyncReference<String> reference = node.create(AsyncReference.class).get();

    expectResume();
    reference.set("Hello world!").thenRun(this::resume);
    await();

    expectResume();
    reference.get().thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await();
  }

  /**
   * Tests compare-and-set.
   */
  public void testCompareAndSet() throws Throwable {
    Servers servers = createCopycats(3, 2);

    Node node1 = servers.active.get(0).create("/test").get();
    AsyncReference<Integer> reference1 = node1.create(AsyncReference.class).get();

    expectResume();
    reference1.set(1).thenRun(this::resume);
    await();

    Node node2 = servers.active.get(0).create("/test").get();
    AsyncReference<Integer> reference2 = node2.create(AsyncReference.class).get();

    expectResume();
    reference2.compareAndSet(1, 2).thenAccept(result -> {
      threadAssertTrue(result);
      resume();
    });
    await();

    expectResume();
    reference2.compareAndSet(1, 3).thenAccept(result -> {
      threadAssertFalse(result);
      resume();
    });
    await();
  }

  /**
   * Creates a Copycat instance.
   */
  private Servers createCopycats(int activeNodes, int passiveNodes) throws Throwable {
    LocalServerRegistry registry = new LocalServerRegistry();

    List<Copycat> active = new ArrayList<>();

    expectResumes(activeNodes);

    Members.Builder builder = Members.builder();
    for (int i = 1; i <= activeNodes; i++) {
      builder.addMember(Member.builder()
        .withId(i)
        .withHost("localhost")
        .withPort(5000 + i)
        .build());
    }

    Members members = builder.build();

    for (int i = 1; i <= activeNodes; i++) {
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

      active.add(copycat);
    }

    await();

    List<Copycat> passive = new ArrayList<>();

    expectResumes(passiveNodes);

    for (int i = activeNodes + 1; i <= activeNodes + passiveNodes; i++) {
      Copycat copycat = CopycatServer.builder()
        .withMemberId(i)
        .withMemberType(Member.Type.PASSIVE)
        .withHost("localhost")
        .withPort(5000 + i)
        .withMembers(members)
        .withTransport(LocalTransport.builder()
          .withRegistry(registry)
          .build())
        .withLog(Log.builder()
          .withStorageLevel(StorageLevel.MEMORY)
          .build())
        .build();

      copycat.open().thenRun(this::resume);

      passive.add(copycat);
    }

    await();

    return new Servers(active, passive);
  }

  private static class Servers {
    private final List<Copycat> active;
    private final List<Copycat> passive;

    private Servers(List<Copycat> active, List<Copycat> passive) {
      this.active = active;
      this.passive = passive;
    }
  }

}
