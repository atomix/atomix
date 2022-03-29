// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.utils;

import java.util.function.Consumer;

/**
 * Quorum helper. Completes and invokes a callback when the number of {@link #succeed()} or
 * {@link #fail()} calls equal the expected quorum count. Not threadsafe.
 */
public class Quorum {
  private final int quorum;
  private int succeeded = 1;
  private int failed;
  private Consumer<Boolean> callback;
  private boolean complete;

  public Quorum(int quorum, Consumer<Boolean> callback) {
    this.quorum = quorum;
    this.callback = callback;
  }

  private void checkComplete() {
    if (!complete && callback != null) {
      if (succeeded >= quorum) {
        complete = true;
        callback.accept(true);
      } else if (failed >= quorum) {
        complete = true;
        callback.accept(false);
      }
    }
  }

  /**
   * Indicates that a call in the quorum succeeded.
   */
  public Quorum succeed() {
    succeeded++;
    checkComplete();
    return this;
  }

  /**
   * Indicates that a call in the quorum failed.
   */
  public Quorum fail() {
    failed++;
    checkComplete();
    return this;
  }

  /**
   * Cancels the quorum. Once this method has been called, the quorum will be marked complete and
   * the handler will never be called.
   */
  public void cancel() {
    callback = null;
    complete = true;
  }

}
