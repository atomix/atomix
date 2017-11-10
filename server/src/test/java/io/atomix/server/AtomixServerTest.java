/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.server;

import io.atomix.cluster.NodeId;
import io.atomix.messaging.netty.NettyMessagingService;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Atomix server test.
 */
public class AtomixServerTest {

  @Test
  public void testParseAddress() throws Exception {
    String[] address = AtomixServer.parseAddress("a:b:c");
    assertEquals(3, address.length);
    try {
      AtomixServer.parseAddress("a:b:c:d");
      fail();
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testParseNodeId() throws Exception {
    assertEquals(NodeId.from(String.format("127.0.0.1:%d", NettyMessagingService.DEFAULT_PORT)), AtomixServer.parseNodeId(new String[]{"127.0.0.1"}));
    assertEquals(NodeId.from("foo"), AtomixServer.parseNodeId(new String[]{"foo"}));
    assertEquals(NodeId.from("127.0.0.1:1234"), AtomixServer.parseNodeId(new String[]{"127.0.0.1", "1234"}));
    assertEquals(NodeId.from("foo"), AtomixServer.parseNodeId(new String[]{"foo", "127.0.0.1", "1234"}));
    assertEquals(NodeId.from("foo"), AtomixServer.parseNodeId(new String[]{"foo", "127.0.0.1"}));
  }

  @Test
  public void testParseEndpoint() throws Exception {
    assertEquals(String.format("127.0.0.1:%d", NettyMessagingService.DEFAULT_PORT), AtomixServer.parseEndpoint(new String[]{"foo"}).toString());
    assertEquals(String.format("127.0.0.1:%d", NettyMessagingService.DEFAULT_PORT), AtomixServer.parseEndpoint(new String[]{"127.0.0.1"}).toString());
    assertEquals(String.format("127.0.0.1:%d", NettyMessagingService.DEFAULT_PORT), AtomixServer.parseEndpoint(new String[]{"foo", "127.0.0.1"}).toString());
    assertEquals("127.0.0.1:1234", AtomixServer.parseEndpoint(new String[]{"127.0.0.1", "1234"}).toString());
    assertEquals("127.0.0.1:1234", AtomixServer.parseEndpoint(new String[]{"foo", "127.0.0.1", "1234"}).toString());
  }

}
