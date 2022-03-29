// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.test.protocol;

import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocolConfig;

/**
 * Test protocol config.
 */
public class TestProtocolConfig extends PrimitiveProtocolConfig<TestProtocolConfig> {
  private String group = "test";
  private int partitions = 3;
  private Partitioner<String> partitioner = Partitioner.MURMUR3;

  @Override
  public PrimitiveProtocol.Type getType() {
    return TestProtocol.TYPE;
  }

  /**
   * Returns the partition group.
   *
   * @return the partition group
   */
  public String getGroup() {
    return group;
  }

  /**
   * Sets the partition group.
   *
   * @param group the partition group
   * @return the test protocol configuration
   */
  public TestProtocolConfig setGroup(String group) {
    this.group = group;
    return this;
  }

  /**
   * Returns the number of partitions to use in tests.
   *
   * @return the number of partitions to use in tests
   */
  public int getPartitions() {
    return partitions;
  }

  /**
   * Sets the number of partitions to use in tests.
   *
   * @param partitions the number of partitions to use in tests
   * @return the test protocol configuration
   */
  public TestProtocolConfig setPartitions(int partitions) {
    this.partitions = partitions;
    return this;
  }

  /**
   * Returns the protocol partitioner.
   *
   * @return the protocol partitioner
   */
  public Partitioner<String> getPartitioner() {
    return partitioner;
  }

  /**
   * Sets the protocol partitioner.
   *
   * @param partitioner the protocol partitioner
   * @return the protocol configuration
   */
  public TestProtocolConfig setPartitioner(Partitioner<String> partitioner) {
    this.partitioner = partitioner;
    return this;
  }
}
