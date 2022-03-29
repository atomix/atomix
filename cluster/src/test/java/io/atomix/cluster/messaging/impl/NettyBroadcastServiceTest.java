// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.messaging.impl;

import io.atomix.cluster.messaging.ManagedBroadcastService;
import io.atomix.utils.net.Address;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.ServerSocket;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Netty broadcast service test.
 */
public class NettyBroadcastServiceTest extends ConcurrentTestCase {

  private static final Logger LOGGER = getLogger(NettyBroadcastServiceTest.class);

  ManagedBroadcastService netty1;
  ManagedBroadcastService netty2;

  Address localAddress1;
  Address localAddress2;
  Address groupAddress;

  @Test
  public void testBroadcast() throws Exception {
    netty1.addListener("test", bytes -> {
      threadAssertEquals(0, bytes.length);
      resume();
    });

    netty2.broadcast("test", new byte[0]);
    await(5000);
  }

  @Before
  public void setUp() throws Exception {
    localAddress1 = Address.from("127.0.0.1", findAvailablePort(5001));
    localAddress2 = Address.from("127.0.0.1", findAvailablePort(5002));
    groupAddress = Address.from("230.0.0.1", findAvailablePort(1234));

    netty1 = (ManagedBroadcastService) NettyBroadcastService.builder()
        .withLocalAddress(localAddress1)
        .withGroupAddress(groupAddress)
        .build()
        .start()
        .join();

    netty2 = (ManagedBroadcastService) NettyBroadcastService.builder()
        .withLocalAddress(localAddress2)
        .withGroupAddress(groupAddress)
        .build()
        .start()
        .join();
  }

  @After
  public void tearDown() throws Exception {
    if (netty1 != null) {
      try {
        netty1.stop().join();
      } catch (Exception e) {
        LOGGER.warn("Failed stopping netty1", e);
      }
    }

    if (netty2 != null) {
      try {
        netty2.stop().join();
      } catch (Exception e) {
        LOGGER.warn("Failed stopping netty2", e);
      }
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
