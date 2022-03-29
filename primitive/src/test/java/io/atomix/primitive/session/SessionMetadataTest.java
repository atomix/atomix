// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.session;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Raft session metadata test.
 */
public class SessionMetadataTest {
  @Test
  public void testRaftSessionMetadata() throws Exception {
    SessionMetadata metadata = new SessionMetadata(1, "foo", "test");
    assertEquals(SessionId.from(1), metadata.sessionId());
    assertEquals("foo", metadata.primitiveName());
    assertEquals("test", metadata.primitiveType());
  }
}
