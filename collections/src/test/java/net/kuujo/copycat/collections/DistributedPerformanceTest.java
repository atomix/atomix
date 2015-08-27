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
import net.kuujo.copycat.CopycatClient;
import net.kuujo.copycat.CopycatReplica;
import net.kuujo.copycat.io.storage.Storage;
import net.kuujo.copycat.io.transport.NettyTransport;
import net.kuujo.copycat.raft.Member;
import net.kuujo.copycat.raft.Members;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Distributed map performance test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class DistributedPerformanceTest extends ConcurrentTestCase {
  private static final File directory = new File("test-logs");

  /**
   * Tests putting and getting a value.
   */
  @SuppressWarnings("unchecked")
  public void testPerformance() throws Throwable {
    Members.Builder builder = Members.builder();
    for (int i = 1; i <= 5; i++) {
      builder.addMember(new Member(i, "localhost", 5000 + i));
    }

    Members members = builder.build();

    for (int i = 1; i <= 3; i++) {
      Copycat copycat = CopycatReplica.builder()
        .withMemberId(i)
        .withMembers(members)
        .withTransport(new NettyTransport(4))
        .withStorage(Storage.builder()
          .withDirectory(new File(directory, "" + i))
          .build())
        .build();

      copycat.open().thenRun(this::resume);
    }

    await(0, 3);

    Copycat copycat = CopycatClient.builder()
      .withMembers(members)
      .withTransport(new NettyTransport(2))
      .build();

    AtomicInteger outstanding = new AtomicInteger();
    AtomicLong counter = new AtomicLong();

    new Thread(() -> {
      try {
        DistributedMap<String, String> map = copycat.open().get().<DistributedMap<String, String>>create("test", DistributedMap.class).get();
        recursivePut(map, outstanding, counter);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }).start();

    for (;;) {
      Thread.sleep(1000);
      System.out.println("Submitted " + counter.get() + " writes with " + outstanding.get() + " writes outstanding");
    }
  }

  /**
   * Recursively submits writes to the cluster.
   */
  private void recursivePut(DistributedMap<String, String> map, AtomicInteger outstanding, AtomicLong counter) {
    map.put("foo", "bar").whenComplete((result, error) -> {
      if (error == null) {
        counter.incrementAndGet();
      }

      outstanding.decrementAndGet();
      recursivePut(map, outstanding, counter);
    });

    if (outstanding.incrementAndGet() < 1000) {
      recursivePut(map, outstanding, counter);
    }
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
