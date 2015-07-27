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
import net.kuujo.copycat.Member;
import net.kuujo.copycat.Members;
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
@SuppressWarnings("unchecked")
public class DistributedReferenceTest extends ConcurrentTestCase {

  /**
   * Tests setting and getting a value.
   */
  @SuppressWarnings("unchecked")
  public void testSetGet() throws Throwable {
    List<Copycat> servers = createCopycats(3);

    Copycat copycat = servers.get(0);

    Node node = copycat.create("/test").get();
    DistributedReference<String> reference = node.create(DistributedReference.class).get();

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
   * Tests setting and getting a value with a change event.
   */
  @SuppressWarnings("unchecked")
  public void testChangeEvent() throws Throwable {
    List<Copycat> servers = createCopycats(3);

    Copycat copycat = servers.get(0);

    Node node = copycat.create("/test").get();
    DistributedReference<String> reference = node.create(DistributedReference.class).get();

    expectResume();
    reference.onChange(value -> {
      threadAssertEquals("Hello world!", value);
      resume();
    }).thenRun(this::resume);
    await();

    expectResumes(2);
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
   * Tests a membership change.
   */
  public void testMembershipChange() throws Throwable {
    LocalServerRegistry registry = new LocalServerRegistry();

    Members initialMembers = Members.builder()
      .addMember(Member.builder()
        .withId(1)
        .withHost("localhost")
        .withPort(5001)
        .build())
      .addMember(Member.builder()
        .withId(2)
        .withHost("localhost")
        .withPort(5002)
        .build())
      .addMember(Member.builder()
        .withId(3)
        .withHost("localhost")
        .withPort(5003)
        .build())
      .build();

    Copycat copycat1 = CopycatServer.builder()
      .withMemberId(1)
      .withMembers(initialMembers)
      .withTransport(LocalTransport.builder()
        .withRegistry(registry)
        .build())
      .withLog(Log.builder()
        .withStorageLevel(StorageLevel.MEMORY)
        .build())
      .build();
    Copycat copycat2 = CopycatServer.builder()
      .withMemberId(2)
      .withMembers(initialMembers)
      .withTransport(LocalTransport.builder()
        .withRegistry(registry)
        .build())
      .withLog(Log.builder()
        .withStorageLevel(StorageLevel.MEMORY)
        .build())
      .build();
    Copycat copycat3 = CopycatServer.builder()
      .withMemberId(3)
      .withMembers(initialMembers)
      .withTransport(LocalTransport.builder()
        .withRegistry(registry)
        .build())
      .withLog(Log.builder()
        .withStorageLevel(StorageLevel.MEMORY)
        .build())
      .build();

    expectResumes(3);
    copycat1.open().thenRun(this::resume);
    copycat2.open().thenRun(this::resume);
    copycat3.open().thenRun(this::resume);
    await();

    Members updatedMembers = Members.builder()
      .addMember(Member.builder()
        .withId(1)
        .withHost("localhost")
        .withPort(5001)
        .build())
      .addMember(Member.builder()
        .withId(2)
        .withHost("localhost")
        .withPort(5002)
        .build())
      .addMember(Member.builder()
        .withId(3)
        .withHost("localhost")
        .withPort(5003)
        .build())
      .addMember(Member.builder()
        .withId(4)
        .withHost("localhost")
        .withPort(5004)
        .build())
      .build();

    Copycat copycat4 = CopycatServer.builder()
      .withMemberId(4)
      .withMembers(updatedMembers)
      .withTransport(LocalTransport.builder()
        .withRegistry(registry)
        .build())
      .withLog(Log.builder()
        .withStorageLevel(StorageLevel.MEMORY)
        .build())
      .build();

    expectResume();
    copycat4.open().thenRun(this::resume);
    await();

    Node node = copycat4.create("/test").get();
    DistributedReference<String> reference = node.create(DistributedReference.class).get();

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
    List<Copycat> servers = createCopycats(3);

    Node node1 = servers.get(0).create("/test").get();
    DistributedReference<Integer> reference1 = node1.create(DistributedReference.class).get();

    expectResume();
    reference1.set(1).thenRun(this::resume);
    await();

    Node node2 = servers.get(0).create("/test").get();
    DistributedReference<Integer> reference2 = node2.create(DistributedReference.class).get();

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
