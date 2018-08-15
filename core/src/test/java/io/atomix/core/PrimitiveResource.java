/*
 * Copyright 2018-present Open Networking Foundation
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

import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.cluster.discovery.MulticastDiscoveryProvider;
import io.atomix.core.profile.Profile;
import io.atomix.protocols.backup.partition.PrimaryBackupPartitionGroup;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.net.Address;
import org.junit.rules.ExternalResource;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.atomix.core.AbstractAtomixTest.DATA_DIR;

/**
 * Atomix primitive resource.
 */
public class PrimitiveResource extends ExternalResource {
  private static final int BASE_PORT = 5000;
  private static final AtomicInteger REFERENCES = new AtomicInteger();
  private static volatile PrimitiveResource instance;

  public static synchronized PrimitiveResource getInstance() {
    if (instance == null) {
      instance = new PrimitiveResource();
    }
    return instance;
  }

  private List<Atomix> servers = new ArrayList<>();
  private List<Atomix> clients = new ArrayList<>();
  private int usedClients;
  private int id = 10;

  /**
   * Returns a new Atomix instance.
   *
   * @return a new Atomix instance.
   */
  public Atomix atomix() throws Exception {
    Atomix instance;
    if (clients.size() > usedClients) {
      instance = clients.get(usedClients);
    } else {
      instance = createAtomix(id++, Arrays.asList(1, 2, 3));
      clients.add(instance);
      instance.start().get(30, TimeUnit.SECONDS);
    }
    usedClients++;
    return instance;
  }

  @Override
  protected void before() throws Throwable {
    if (REFERENCES.getAndIncrement() == 0) {
      startup();
    }
    usedClients = 0;
  }

  private void startup() throws Exception {
    AbstractAtomixTest.setupAtomix();
    BiFunction<AtomixBuilder, Integer, Atomix> build = (builder, id) ->
        builder.withManagementGroup(RaftPartitionGroup.builder("system")
            .withNumPartitions(1)
            .withMembers("1", "2", "3")
            .withDataDirectory(new File(new File(DATA_DIR, "system"), String.valueOf(id)))
            .build())
            .addPartitionGroup(RaftPartitionGroup.builder("raft")
                .withNumPartitions(3)
                .withMembers("1", "2", "3")
                .withDataDirectory(new File(new File(DATA_DIR, "raft"), String.valueOf(id)))
                .build())
            .addPartitionGroup(PrimaryBackupPartitionGroup.builder("primary-backup")
                .withNumPartitions(7)
                .build())
            .build();
    servers = new ArrayList<>();
    servers.add(createAtomix(1, Arrays.asList(1, 2, 3), builder -> build.apply(builder, 1)));
    servers.add(createAtomix(2, Arrays.asList(1, 2, 3), builder -> build.apply(builder, 2)));
    servers.add(createAtomix(3, Arrays.asList(1, 2, 3), builder -> build.apply(builder, 3)));
    List<CompletableFuture<Atomix>> futures = servers.stream().map(a -> a.start().thenApply(v -> a)).collect(Collectors.toList());
    Futures.allOf(futures).get(30, TimeUnit.SECONDS);
  }

  @Override
  protected void after() {
    if (REFERENCES.decrementAndGet() == 0) {
      shutdown();
    }
  }

  private void shutdown() {
    try {
      Futures.allOf(clients.stream().map(Atomix::stop)).get(1, TimeUnit.MINUTES);
    } catch (Exception e) {
      e.printStackTrace();
    }

    try {
      Futures.allOf(servers.stream().map(Atomix::stop)).get(1, TimeUnit.MINUTES);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Creates an Atomix instance.
   */
  protected static Atomix createAtomix(int id, List<Integer> bootstrapIds, Profile... profiles) {
    return createAtomix(id, bootstrapIds, new Properties(), profiles);
  }

  /**
   * Creates an Atomix instance.
   */
  protected static Atomix createAtomix(int id, List<Integer> bootstrapIds, Properties properties, Profile... profiles) {
    return createAtomix(id, bootstrapIds, properties, b -> b.withProfiles(profiles).build());
  }

  /**
   * Creates an Atomix instance.
   */
  protected static Atomix createAtomix(int id, List<Integer> bootstrapIds, Function<AtomixBuilder, Atomix> builderFunction) {
    return createAtomix(id, bootstrapIds, new Properties(), builderFunction);
  }

  /**
   * Creates an Atomix instance.
   */
  protected static Atomix createAtomix(int id, List<Integer> bootstrapIds, Properties properties, Function<AtomixBuilder, Atomix> builderFunction) {
    return builderFunction.apply(buildAtomix(id, bootstrapIds, properties));
  }

  /**
   * Creates an Atomix instance.
   */
  protected static AtomixBuilder buildAtomix(int id, List<Integer> memberIds, Properties properties) {
    Collection<Node> nodes = memberIds.stream()
        .map(memberId -> Node.builder()
            .withId(String.valueOf(id))
            .withAddress(Address.from("localhost", BASE_PORT + memberId))
            .build())
        .collect(Collectors.toList());

    return Atomix.builder()
        .withClusterId("test")
        .withMemberId(String.valueOf(id))
        .withAddress("localhost", BASE_PORT + id)
        .withProperties(properties)
        .withMulticastEnabled()
        .withMembershipProvider(!nodes.isEmpty() ? new BootstrapDiscoveryProvider(nodes) : new MulticastDiscoveryProvider());
  }
}
