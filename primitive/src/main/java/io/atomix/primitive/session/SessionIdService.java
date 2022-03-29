// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.session;

import java.util.concurrent.CompletableFuture;

/**
 * Globally unique session ID provider.
 */
public interface SessionIdService {

  /**
   * Returns the next unique session identifier.
   *
   * @return the next unique session identifier
   */
  CompletableFuture<SessionId> nextSessionId();

}
