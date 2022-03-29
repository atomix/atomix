// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol;

import io.atomix.primitive.log.LogClient;
import io.atomix.primitive.partition.PartitionService;

/**
 * Log replication based primitive protocol.
 */
public interface LogProtocol extends ProxyProtocol {

  /**
   * Returns the protocol partition group name.
   *
   * @return the protocol partition group name
   */
  String group();

  /**
   * Returns a new log client.
   *
   * @param partitionService the partition service
   * @return a new log client
   */
  LogClient newClient(PartitionService partitionService);

}
