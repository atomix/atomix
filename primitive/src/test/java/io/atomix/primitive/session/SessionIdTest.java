// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.session;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Session identifier test.
 */
public class SessionIdTest {
  @Test
  public void testSessionId() throws Exception {
    SessionId sessionId = SessionId.from(1);
    assertEquals(Long.valueOf(1), sessionId.id());
  }
}
