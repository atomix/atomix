// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.session;

import io.atomix.utils.AbstractIdentifier;

/**
 * Session identifier.
 */
public class SessionId extends AbstractIdentifier<Long> {

  /**
   * Returns a new session ID from the given identifier.
   *
   * @param id the identifier from which to create a session ID
   * @return a new session identifier
   */
  public static SessionId from(long id) {
    return new SessionId(id);
  }

  protected SessionId() {
  }

  public SessionId(Long value) {
    super(value);
  }
}
