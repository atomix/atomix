// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.test.protocol;

import io.atomix.primitive.protocol.PrimitiveProtocolBuilder;

/**
 * Test protocol builder.
 */
public class TestProtocolBuilder extends PrimitiveProtocolBuilder<TestProtocolBuilder, TestProtocolConfig, TestProtocol> {
  TestProtocolBuilder(TestProtocolConfig config) {
    super(config);
  }

  /**
   * Sets the number of test partitions.
   *
   * @param partitions the number of test partitions
   * @return the test protocol builder
   */
  public TestProtocolBuilder withNumPartitions(int partitions) {
    config.setPartitions(partitions);
    return this;
  }

  @Override
  public TestProtocol build() {
    return new TestProtocol(config);
  }
}
