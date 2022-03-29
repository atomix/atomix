// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0


package io.atomix.primitive.session;

import io.atomix.primitive.event.PrimitiveEvent;

/**
 * Provides a set of active server sessions.
 * <p>
 * Server state machines can use the {@code Sessions} object to access the list of sessions currently open to the
 * state machine. Session sets are guaranteed to be deterministic. All state machines will see the same set of
 * open sessions at the same point in the log except in cases where a session has already been closed and removed.
 * If a session has already been closed on another server, the session is guaranteed to have been expired on all
 * servers and thus operations like {@link Session#publish(PrimitiveEvent)} are effectively no-ops.
 */
public interface Sessions extends Iterable<Session> {

  /**
   * Returns a session by session ID.
   *
   * @param sessionId The session ID.
   * @return The session or {@code null} if no session with the given {@code sessionId} exists.
   */
  Session getSession(long sessionId);

  /**
   * Adds a listener to the sessions.
   *
   * @param listener The listener to add.
   * @return The sessions.
   * @throws NullPointerException if the session {@code listener} is {@code null}
   */
  Sessions addListener(SessionListener listener);

  /**
   * Removes a listener from the sessions.
   *
   * @param listener The listener to remove.
   * @return The sessions.
   */
  Sessions removeListener(SessionListener listener);

}
