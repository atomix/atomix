/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.core;

import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.protocols.backup.partition.PrimaryBackupPartitionGroup;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Base Atomix test.
 */
public abstract class AbstractPrimitiveTest extends AbstractAtomixTest {
  private static List<Atomix> servers;
  private static List<Atomix> clients;
  private static int id = 10;

  /**
   * Returns the primitive protocol with which to test.
   *
   * @return the protocol with which to test
   */
  protected abstract PrimitiveProtocol protocol();

  /**
   * Returns a new Atomix instance.
   *
   * @return a new Atomix instance.
   */
  protected Atomix atomix() throws Exception {
    Atomix instance = createAtomix(id++, Arrays.asList(1, 2, 3));
    clients.add(instance);
    instance.start().get(30, TimeUnit.SECONDS);
    return instance;
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    AbstractAtomixTest.setupAtomix();
    Function<Atomix.Builder, Atomix> build = builder ->
        builder.withManagementGroup(RaftPartitionGroup.builder("system")
            .withNumPartitions(1)
            .withMembers("1", "2", "3")
            .build())
            .addPartitionGroup(RaftPartitionGroup.builder("raft")
                .withNumPartitions(3)
                .withMembers("1", "2", "3")
                .build())
            .addPartitionGroup(PrimaryBackupPartitionGroup.builder("data")
                .withNumPartitions(7)
                .build())
            .build();
    servers = new ArrayList<>();
    servers.add(createAtomix(1, Arrays.asList(1, 2, 3), build));
    servers.add(createAtomix(2, Arrays.asList(1, 2, 3), build));
    servers.add(createAtomix(3, Arrays.asList(1, 2, 3), build));
    List<CompletableFuture<Atomix>> futures = servers.stream().map(a -> a.start().thenApply(v -> a)).collect(Collectors.toList());
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get(30, TimeUnit.SECONDS);
  }

  @AfterClass
  public static void teardownCluster() throws Exception {
    List<CompletableFuture<Void>> futures = servers.stream().map(Atomix::stop).collect(Collectors.toList());
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
    } catch (Exception e) {
      // Do nothing
    }
    AbstractAtomixTest.teardownAtomix();
  }

  @Before
  public void setupTest() throws Exception {
    clients = new ArrayList<>();
    id = 10;
  }

  @After
  public void teardownTest() throws Exception {
    List<CompletableFuture<Void>> futures = clients.stream().map(Atomix::stop).collect(Collectors.toList());
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
    } catch (Exception e) {
      // Do nothing
    }
  }
}
