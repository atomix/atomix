// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core;

import io.atomix.core.test.TestAtomixFactory;
import io.atomix.core.test.protocol.TestProtocol;
import io.atomix.primitive.protocol.ProxyProtocol;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Base Atomix test.
 */
public abstract class AbstractPrimitiveTest {
  private List<Atomix> members;
  private TestAtomixFactory atomixFactory;
  private TestProtocol protocol;

  /**
   * Returns the primitive protocol with which to test.
   *
   * @return the protocol with which to test
   */
  protected ProxyProtocol protocol() {
    return protocol;
  }

  /**
   * Returns a new Atomix instance.
   *
   * @return a new Atomix instance.
   */
  protected Atomix atomix() throws Exception {
    Atomix instance = createAtomix();
    instance.start().get(30, TimeUnit.SECONDS);
    return instance;
  }

  /**
   * Creates a new Atomix instance.
   *
   * @return the Atomix instance
   */
  private Atomix createAtomix() {
    Atomix atomix = atomixFactory.newInstance();
    members.add(atomix);
    return atomix;
  }

  @Before
  public void setupTest() throws Exception {
    members = new ArrayList<>();
    atomixFactory = new TestAtomixFactory();
    protocol = TestProtocol.builder()
        .withNumPartitions(3)
        .build();
  }

  @After
  public void teardownTest() throws Exception {
    List<CompletableFuture<Void>> futures = members.stream().map(Atomix::stop).collect(Collectors.toList());
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get(30, TimeUnit.SECONDS);
    } catch (Exception e) {
      // Do nothing
    } finally {
      protocol.close();
    }
  }
}
