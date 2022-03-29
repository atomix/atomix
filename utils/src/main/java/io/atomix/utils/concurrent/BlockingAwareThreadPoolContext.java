// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.concurrent;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Blocking aware thread pool context.
 */
public class BlockingAwareThreadPoolContext extends ThreadPoolContext {
  public BlockingAwareThreadPoolContext(ScheduledExecutorService parent) {
    super(parent);
  }

  @Override
  public void execute(Runnable command) {
    if (isBlocked()) {
      parent.execute(command);
    } else {
      super.execute(command);
    }
  }
}
