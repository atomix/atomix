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
import net.kuujo.copycat.CopycatReplica;
import net.kuujo.copycat.io.storage.Storage;
import net.kuujo.copycat.io.transport.LocalServerRegistry;
import net.kuujo.copycat.io.transport.LocalTransport;
import net.kuujo.copycat.raft.Member;
import net.kuujo.copycat.raft.Members;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Distributed atomic value test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
@SuppressWarnings("unchecked")
public class DistributedAtomicValueTest extends ConcurrentTestCase {
  private static final File directory = new File("test-logs");

  /**
   * Tests setting and getting a value.
   */
  @SuppressWarnings("unchecked")
  public void testSetGet() throws Throwable {
    List<Copycat> servers = createCopycats(3);

    Copycat copycat = servers.get(0);

    DistributedAtomicValue<String> reference = copycat.create("test", DistributedAtomicValue.class).get();

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

    DistributedAtomicValue<String> reference = copycat.create("test", DistributedAtomicValue.class).get();

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
      .addMember(new Member(1, "localhost", 5001))
      .addMember(new Member(2, "localhost", 5002))
      .addMember(new Member(3, "localhost", 5003))
      .build();

    Copycat copycat1 = CopycatReplica.builder()
      .withMemberId(1)
      .withMembers(initialMembers)
      .withTransport(new LocalTransport(registry))
      .withStorage(Storage.builder()
        .withDirectory(new File(directory, "1"))
        .build())
      .build();
    Copycat copycat2 = CopycatReplica.builder()
      .withMemberId(2)
      .withMembers(initialMembers)
      .withTransport(new LocalTransport(registry))
      .withStorage(Storage.builder()
        .withDirectory(new File(directory, "2"))
        .build())
      .build();
    Copycat copycat3 = CopycatReplica.builder()
      .withMemberId(3)
      .withMembers(initialMembers)
      .withTransport(new LocalTransport(registry))
      .withStorage(Storage.builder()
        .withDirectory(new File(directory, "3"))
        .build())
      .build();

    expectResumes(3);
    copycat1.open().thenRun(this::resume);
    copycat2.open().thenRun(this::resume);
    copycat3.open().thenRun(this::resume);
    await();

    Members updatedMembers = Members.builder()
      .addMember(new Member(1, "localhost", 5001))
      .addMember(new Member(2, "localhost", 5002))
      .addMember(new Member(3, "localhost", 5003))
      .addMember(new Member(4, "localhost", 5004))
      .build();

    Copycat copycat4 = CopycatReplica.builder()
      .withMemberId(4)
      .withMembers(updatedMembers)
      .withTransport(new LocalTransport(registry))
      .withStorage(Storage.builder()
        .withDirectory(new File(directory, "4"))
        .build())
      .build();

    expectResume();
    copycat4.open().thenRun(this::resume);
    await();

    DistributedAtomicValue<String> reference = copycat4.create("test", DistributedAtomicValue.class).get();

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

    DistributedAtomicValue<Integer> reference1 = servers.get(0).create("test", DistributedAtomicValue.class).get();

    expectResume();
    reference1.set(1).thenRun(this::resume);
    await();

    DistributedAtomicValue<Integer> reference2 = servers.get(0).create("test", DistributedAtomicValue.class).get();

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
      builder.addMember(new Member(i, "localhost", 5000 + i));
    }

    Members members = builder.build();

    for (int i = 1; i <= nodes; i++) {
      Copycat copycat = CopycatReplica.builder()
        .withMemberId(i)
        .withMembers(members)
        .withTransport(new LocalTransport(registry))
        .withStorage(Storage.builder()
          .withDirectory(new File(directory, "" + i))
          .build())
        .build();

      copycat.open().thenRun(this::resume);

      copycats.add(copycat);
    }

    await();

    return copycats;
  }

  @BeforeMethod
  @AfterMethod
  public void clearTests() throws IOException {
    deleteDirectory(directory);
  }

  /**
   * Deletes a directory recursively.
   */
  private void deleteDirectory(File directory) throws IOException {
    if (directory.exists()) {
      File[] files = directory.listFiles();
      if (files != null) {
        for (File file : files) {
          if (file.isDirectory()) {
            deleteDirectory(file);
          } else {
            Files.delete(file.toPath());
          }
        }
      }
      Files.delete(directory.toPath());
    }
  }

}
