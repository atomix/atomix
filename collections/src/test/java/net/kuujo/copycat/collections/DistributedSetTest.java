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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Distributed map test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class DistributedSetTest extends ConcurrentTestCase {
  private static final File directory = new File("test-logs");

  /**
   * Tests adding and removing members from a set.
   */
  public void testSetAddRemove() throws Throwable {
    List<Copycat> copycats = createCopycats(3);

    Copycat copycat1 = copycats.get(0);
    Copycat copycat2 = copycats.get(1);

    Node node1 = copycat1.create("/test").get();
    DistributedSet<String> set1 = node1.create(DistributedSet.class).get();
    assertFalse(set1.contains("Hello world!").get());

    Node node2 = copycat2.create("/test").get();
    DistributedSet<String> set2 = node2.create(DistributedSet.class).get();
    assertFalse(set2.contains("Hello world!").get());

    set1.add("Hello world!").join();
    assertTrue(set1.contains("Hello world!").get());
    assertTrue(set2.contains("Hello world!").get());

    set2.remove("Hello world!").join();
    assertFalse(set1.contains("Hello world!").get());
    assertFalse(set2.contains("Hello world!").get());
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
