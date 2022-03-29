// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster;

import io.atomix.cluster.messaging.BroadcastService;
import io.atomix.cluster.messaging.MessagingService;
import io.atomix.cluster.messaging.UnicastService;

/**
 * Cluster bootstrap service.
 * <p>
 * This service provides the low level APIs that can be used to bootstrap a cluster.
 */
public interface BootstrapService {

  /**
   * Returns the cluster messaging service.
   *
   * @return the cluster messaging service
   */
  MessagingService getMessagingService();

  /**
   * Returns the cluster unicast service.
   *
   * @return the cluster unicast service
   */
  UnicastService getUnicastService();

  /**
   * Returns the cluster broadcast service
   *
   * @return the cluster broadcast service
   */
  BroadcastService getBroadcastService();

}
