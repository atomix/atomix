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
package io.atomix.cluster.messaging.impl;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.TlsConfig;
import io.atomix.utils.net.Address;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.time.Duration;
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

import static org.junit.Assert.*;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Netty messaging service test.
 */
public class NettyMessagingServiceTest {

  private static final Logger LOGGER = getLogger(NettyMessagingServiceTest.class);

  ManagedMessagingService netty1;
  ManagedMessagingService netty2;
  ManagedMessagingService netty3;
  ManagedMessagingService netty4;

  private static final String IP_STRING = "127.0.0.1";

  Address ep1;
  Address ep2;
  Address ep3;
  Address ep4;
  Address invalidAddress;

  private static final String KEY_STORE = "src/test/resources/atomix.keystore";
  private static final String KEY_STORE_PSW = "atomix";

  private static final String P12_KEY_STORE = "src/test/resources/atomix.p12";
  private static final String P12_KEY_STORE_PSW = "kpchangemenow";

  @Before
  public void setUp() throws Exception {
    ep1 = Address.from(findAvailablePort(5001));
    netty1 = (ManagedMessagingService) new NettyMessagingService("test", ep1, new MessagingConfig()).start().join();

    ep2 = Address.from(findAvailablePort(5003));
    netty2 = (ManagedMessagingService) new NettyMessagingService("test", ep2, new MessagingConfig()).start().join();

    invalidAddress = Address.from(IP_STRING, 5003);
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

    if (netty3 != null) {
      try {
        netty3.stop().join();
      } catch (Exception e) {
        LOGGER.warn("Failed stopping netty3", e);
      }
    }

    if (netty4 != null) {
      try {
        netty4.stop().join();
      } catch (Exception e) {
        LOGGER.warn("Failed stopping netty4", e);
      }
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
    response = netty1.sendAsync(invalidAddress, subject, "hello world".getBytes());
    response.whenComplete((r, e) -> {
      assertNotNull(e);
      assertTrue(e instanceof ConnectException);
      latch2.countDown();
    });
    Uninterruptibles.awaitUninterruptibly(latch2);
  }

  @Test
  public void testSendAndReceive() {
    String subject = nextSubject();
    AtomicBoolean handlerInvoked = new AtomicBoolean(false);
    AtomicReference<byte[]> request = new AtomicReference<>();
    AtomicReference<Address> sender = new AtomicReference<>();

    BiFunction<Address, byte[], byte[]> handler = (ep, data) -> {
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
    assertEquals(ep1.address(), sender.get().address());
  }

  @Test
  public void testTransientSendAndReceive() {
    String subject = nextSubject();
    AtomicBoolean handlerInvoked = new AtomicBoolean(false);
    AtomicReference<byte[]> request = new AtomicReference<>();
    AtomicReference<Address> sender = new AtomicReference<>();

    BiFunction<Address, byte[], byte[]> handler = (ep, data) -> {
      handlerInvoked.set(true);
      sender.set(ep);
      request.set(data);
      return "hello there".getBytes();
    };
    netty2.registerHandler(subject, handler, MoreExecutors.directExecutor());

    CompletableFuture<byte[]> response = netty1.sendAndReceive(ep2, subject, "hello world".getBytes(), false);
    assertTrue(Arrays.equals("hello there".getBytes(), response.join()));
    assertTrue(handlerInvoked.get());
    assertTrue(Arrays.equals(request.get(), "hello world".getBytes()));
    assertEquals(ep1.address(), sender.get().address());
  }

  @Test
  public void testSendAndReceiveWithFixedTimeout() {
    String subject = nextSubject();
    BiFunction<Address, byte[], CompletableFuture<byte[]>> handler = (ep, payload) -> new CompletableFuture<>();
    netty2.registerHandler(subject, handler);

    try {
      netty1.sendAndReceive(ep2, subject, "hello world".getBytes(), Duration.ofSeconds(1)).join();
      fail();
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof TimeoutException);
    }
  }

  @Test
  public void testSendAndReceiveWithDynamicTimeout() {
    String subject = nextSubject();
    BiFunction<Address, byte[], CompletableFuture<byte[]>> handler = (ep, payload) -> new CompletableFuture<>();
    netty2.registerHandler(subject, handler);

    try {
      netty1.sendAndReceive(ep2, subject, "hello world".getBytes()).join();
      fail();
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof TimeoutException);
    }
  }

  @Test
  @Ignore
  public void testSendAndReceiveWithExecutor() {
    String subject = nextSubject();
    ExecutorService completionExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "completion-thread"));
    ExecutorService handlerExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "handler-thread"));
    AtomicReference<String> handlerThreadName = new AtomicReference<>();
    AtomicReference<String> completionThreadName = new AtomicReference<>();

    final CountDownLatch latch = new CountDownLatch(1);

    BiFunction<Address, byte[], byte[]> handler = (ep, data) -> {
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

  /**
   * Test tls configuration with a password protected cert that expires in 100+ years stored in keystore
   *
   * openssl req -newkey rsa:4096 -nodes -keyout key.pem -x509 -days 365000 -subj "/CN=atomix.io" -out certificate.pem
   * -passout pass:"temporarypassword"
   * openssl pkcs12 -export -in certificate.pem -inkey key.pem -out atomix.pk12 -name "atomix"
   * -password pass:temporarypassword
   * keytool -importkeystore -srckeystore atomix.pk12 -destkeystore atomix.p12 -srcstoretype PKCS12
   * -deststoretype pkcs12 -srcstorepass temporarypassword -deststorepass kpchangemenow -destkeypass kpchangemenow
   * openssl pkcs12 -in atomix.p12 -out atomix.pem
   * keytool -importcert -file atomix.pem -keystore atomix.keystore
   */
  @Test
  public void testTlsConfig() {
    // To test tls configuration
    ep3 = Address.from(findAvailablePort(5004));
    TlsConfig tlsConfig = new TlsConfig().setEnabled(true)
            .setKeyStore(KEY_STORE)
            .setTrustStore(KEY_STORE)
            .setKeyStorePassword(KEY_STORE_PSW)
            .setTrustStorePassword(KEY_STORE_PSW);
    MessagingConfig messagingConfig = new MessagingConfig().setTlsConfig(tlsConfig);
    netty3 = (ManagedMessagingService) new NettyMessagingService("test", ep3, messagingConfig).start().join();

    assertTrue(((NettyMessagingService) netty3).enableNettyTls);
  }

  /**
   * Test tls configuration with a password protected cert that expires in 100+ years stored in a p12 keystore
   *
   * openssl req -newkey rsa:4096 -nodes -keyout key.pem -x509 -days 365000 -subj "/CN=atomix.io" -out certificate.pem
   * -passout pass:"temporarypassword"
   * openssl pkcs12 -export -in certificate.pem -inkey key.pem -out atomix.pk12 -name "atomix"
   * -password pass:temporarypassword
   * keytool -importkeystore -srckeystore atomix.pk12 -destkeystore atomix.p12 -srcstoretype PKCS12
   * -deststoretype pkcs12 -srcstorepass temporarypassword -deststorepass kpchangemenow -destkeypass kpchangemenow
   */
  @Test
  public void testTlsConfigP12() {
    // To test tls configuration with p12 keystore
    ep4 = Address.from(findAvailablePort(5004));
    TlsConfig tlsConfig = new TlsConfig().setEnabled(true)
            .setKeyStore(P12_KEY_STORE)
            .setTrustStore(P12_KEY_STORE)
            .setKeyStorePassword(P12_KEY_STORE_PSW)
            .setTrustStorePassword(P12_KEY_STORE_PSW);
    MessagingConfig messagingConfig = new MessagingConfig().setTlsConfig(tlsConfig);
    netty4 = (ManagedMessagingService) new NettyMessagingService("test", ep4, messagingConfig).start().join();

    assertTrue(((NettyMessagingService) netty4).enableNettyTls);
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
