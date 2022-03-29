// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0


package io.atomix.utils.concurrent;

/**
 * Scheduled task.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Scheduled {

  /**
   * Cancels the scheduled task.
   */
  void cancel();

}
