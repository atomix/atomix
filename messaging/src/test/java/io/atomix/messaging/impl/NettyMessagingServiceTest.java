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
package io.atomix.messaging.impl;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import io.atomix.messaging.Endpoint;
import io.atomix.messaging.ManagedMessagingService;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Netty messaging service test.
 */
public class NettyMessagingServiceTest {

  ManagedMessagingService netty1;
  ManagedMessagingService netty2;

  private static final String IP_STRING = "127.0.0.1";

  Endpoint ep1;
  Endpoint ep2;
  Endpoint invalidEndPoint;

  @Before
  public void setUp() throws Exception {
    ep1 = new Endpoint(InetAddress.getByName("127.0.0.1"), findAvailablePort(5001));
    netty1 = (ManagedMessagingService) NettyMessagingService.builder()
        .withEndpoint(ep1)
        .build()
        .start()
        .join();

    ep2 = new Endpoint(InetAddress.getByName("127.0.0.1"), findAvailablePort(5003));
    netty2 = (ManagedMessagingService) NettyMessagingService.builder()
        .withEndpoint(ep2)
        .build()
        .start()
        .join();

    invalidEndPoint = new Endpoint(InetAddress.getByName(IP_STRING), 5003);
  }

  /**
   * Returns a random String to be used as a test subject.
   * @return string
   */
  private String nextSubject() {
    return UUID.randomUUID().toString();
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

  @Test
  public void testSendAsync() {
    String subject = nextSubject();
    CountDownLatch latch1 = new CountDownLatch(1);
    CompletableFuture<Void> response = netty1.sendAsync(ep2, subject, "hello world".getBytes());
    response.whenComplete((r, e) -> {
      assertNull(e);
      latch1.countDown();
    });
    Uninterruptibles.awaitUninterruptibly(latch1);

    CountDownLatch latch2 = new CountDownLatch(1);
    response = netty1.sendAsync(invalidEndPoint, subject, "hello world".getBytes());
    response.whenComplete((r, e) -> {
      assertNotNull(e);
      assertTrue(e instanceof ConnectException);
      latch2.countDown();
    });
    Uninterruptibles.awaitUninterruptibly(latch2);
  }

  @Test
  @Ignore // FIXME disabled on 9/29/16 due to random failures
  public void testSendAndReceive() {
    String subject = nextSubject();
    AtomicBoolean handlerInvoked = new AtomicBoolean(false);
    AtomicReference<byte[]> request = new AtomicReference<>();
    AtomicReference<Endpoint> sender = new AtomicReference<>();

    BiFunction<Endpoint, byte[], byte[]> handler = (ep, data) -> {
      handlerInvoked.set(true);
      sender.set(ep);
      request.set(data);
      return "hello there".getBytes();
    };
    netty2.registerHandler(subject, handler, MoreExecutors.directExecutor());

    CompletableFuture<byte[]> response = netty1.sendAndReceive(ep2, subject, "hello world".getBytes());
    assertTrue(Arrays.equals("hello there".getBytes(), response.join()));
    assertTrue(handlerInvoked.get());
    assertTrue(Arrays.equals(request.get(), "hello world".getBytes()));
    assertEquals(ep1, sender.get());
  }

  @Test
  public void testSendTimeout() {
    String subject = nextSubject();
    BiFunction<Endpoint, byte[], CompletableFuture<byte[]>> handler = (ep, payload) -> new CompletableFuture<>();
    netty2.registerHandler(subject, handler);

    try {
      netty1.sendAndReceive(ep2, subject, "hello world".getBytes()).join();
      fail();
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof TimeoutException);
    }
  }

  /*
   * Supplies executors when registering a handler and calling sendAndReceive and verifies the request handling
   * and response completion occurs on the expected thread.
   */
  @Test
  @Ignore
  public void testSendAndReceiveWithExecutor() {
    String subject = nextSubject();
    ExecutorService completionExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "completion-thread"));
    ExecutorService handlerExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "handler-thread"));
    AtomicReference<String> handlerThreadName = new AtomicReference<>();
    AtomicReference<String> completionThreadName = new AtomicReference<>();

    final CountDownLatch latch = new CountDownLatch(1);

    BiFunction<Endpoint, byte[], byte[]> handler = (ep, data) -> {
      handlerThreadName.set(Thread.currentThread().getName());
      try {
        latch.await();
      } catch (InterruptedException e1) {
        Thread.currentThread().interrupt();
        fail("InterruptedException");
      }
      return "hello there".getBytes();
    };
    netty2.registerHandler(subject, handler, handlerExecutor);

    CompletableFuture<byte[]> response = netty1.sendAndReceive(ep2,
        subject,
        "hello world".getBytes(),
        completionExecutor);
    response.whenComplete((r, e) -> {
      completionThreadName.set(Thread.currentThread().getName());
    });
    latch.countDown();

    // Verify that the message was request handling and response completion happens on the correct thread.
    assertTrue(Arrays.equals("hello there".getBytes(), response.join()));
    assertEquals("completion-thread", completionThreadName.get());
    assertEquals("handler-thread", handlerThreadName.get());
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
