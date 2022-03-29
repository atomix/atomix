// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.messaging;

import java.util.concurrent.CompletableFuture;

/**
 * {@link ClusterEventService} subscription context.
 * <p>
 * The subscription represents a node's subscription to a specific topic. A {@code Subscription} instance is returned
 * once an {@link ClusterEventService} subscription has been propagated. The subscription context can be used to
 * unsubscribe the node from the given {@link #topic()} by calling {@link #close()}.
 */
public interface Subscription {

  /**
   * Returns the subscription topic.
   *
   * @return the topic to which the subscriber is subscribed
   */
  String topic();

  /**
   * Closes the subscription, causing it to be unregistered.
   * <p>
   * When the subscription is closed, the subscriber will be unregistered and the change will be propagated to all
   * the members of the cluster. The returned future will be completed once the change has been propagated to all nodes.
   *
   * @return a future to be completed once the subscription has been closed
   */
  CompletableFuture<Void> close();

}
