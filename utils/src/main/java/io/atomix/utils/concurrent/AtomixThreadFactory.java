// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.concurrent;

import java.util.concurrent.ThreadFactory;

/**
 * Named thread factory.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AtomixThreadFactory implements ThreadFactory {
  @Override
  public Thread newThread(Runnable r) {
    return new AtomixThread(r);
  }
}
