/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.messaging.impl;

import io.atomix.messaging.Endpoint;
import io.atomix.messaging.ManagedBroadcastService;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

/**
 * Netty broadcast service test.
 */
public class NettyBroadcastServiceTest extends ConcurrentTestCase {

  ManagedBroadcastService netty1;
  ManagedBroadcastService netty2;

  Endpoint localEndpoint1;
  Endpoint localEndpoint2;
  Endpoint groupEndpoint;

  @Test
  public void testBroadcast() throws Exception {
    netty1.addListener(bytes -> {
      threadAssertEquals(0, bytes.length);
      resume();
    });

    netty2.broadcast(new byte[0]);
    await(5000);
  }

  @Before
  public void setUp() throws Exception {
    localEndpoint1 = new Endpoint(InetAddress.getByName("127.0.0.1"), findAvailablePort(5001));
    localEndpoint2 = new Endpoint(InetAddress.getByName("127.0.0.1"), findAvailablePort(5001));
    groupEndpoint = new Endpoint(InetAddress.getByName("230.0.0.1"), findAvailablePort(1234));

    netty1 = (ManagedBroadcastService) NettyBroadcastService.builder()
        .withLocalEndpoint(localEndpoint1)
        .withGroupEndpoint(groupEndpoint)
        .build()
        .start()
        .join();

    netty2 = (ManagedBroadcastService) NettyBroadcastService.builder()
        .withLocalEndpoint(localEndpoint2)
        .withGroupEndpoint(groupEndpoint)
        .build()
        .start()
        .join();
  }

  @After
  public void tearDown() throws Exception {
    if (netty1 != null) {
      netty1.stop();
    }

    if (netty2 != null) {
      netty2.stop();
    }
  }

  private static int findAvailablePort(int defaultPort) {
    try {
      ServerSocket socket = new ServerSocket(0);
      socket.setReuseAddress(true);
      int port = socket.getLocalPort();
      socket.close();
      return port;
    } catch (IOException ex) {
      return defaultPort;
    }
  }
}
