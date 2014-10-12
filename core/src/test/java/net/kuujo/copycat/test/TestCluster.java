/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.test;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.LocalClusterConfig;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.protocol.AsyncLocalProtocol;
import net.kuujo.copycat.spi.protocol.AsyncProtocol;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Test cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TestCluster {
  private static final AsyncProtocol<?> DEFAULT_PROTOCOL = new AsyncLocalProtocol();
  private final List<TestNode> nodes = new ArrayList<>();
  private Supplier<AsyncProtocol<?>> protocolFactory = () -> DEFAULT_PROTOCOL;

  /**
   * Sets a protocol factory.
   *
   * @param factory The protocol factory.
   * @return The test cluster.
   */
  public TestCluster withProtocolFactory(Supplier<AsyncProtocol<?>> factory) {
    this.protocolFactory = factory;
    return this;
  }

  /**
   * Adds a test node to the cluster.
   *
   * @param node The test node to add.
   * @return The test cluster.
   */
  public TestCluster addNode(TestNode node) {
    nodes.add(node);
    return this;
  }

  /**
   * Removes a test node from the cluster.
   *
   * @param node The test node to remove.
   * @return The test cluster.
   */
  public TestCluster removeNode(TestNode node) {
    nodes.remove(node);
    return this;
  }

  /**
   * Synchronously starts the test cluster.
   */
  public void start() {
    nodes.forEach(node -> {
      LocalClusterConfig config = new LocalClusterConfig();
      config.setLocalMember(node.member());
      nodes.forEach(n -> {
        if (!n.id().equals(node.id())) {
          config.addRemoteMember(n.member());
        }
      });
      node.start(new Cluster<Member>(config));
    });
  }

}
