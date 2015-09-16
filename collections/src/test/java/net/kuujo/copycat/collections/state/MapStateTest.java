/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.collections.state;

import net.kuujo.catalog.client.session.Session;
import net.kuujo.catalog.server.StateMachine;
import net.kuujo.catalog.server.state.StateMachineTestCase;
import net.kuujo.copycat.PersistenceMode;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Distributed map state machine test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class MapStateTest extends StateMachineTestCase {

  @Override
  protected StateMachine createStateMachine() {
    return new MapState();
  }

  /**
   * Tests putting and getting a value in a map.
   */
  public void testMapPutGet() throws Throwable {
    long timestamp = System.currentTimeMillis();
    Session session = register(1, timestamp);
    assertNull(apply(2, ++timestamp, session, MapCommands.Put.builder().withKey("foo").withValue("Hello world!").build()));
    assertEquals(apply(++timestamp, session, MapCommands.Get.builder().withKey("foo").build()), "Hello world!");
    assertEquals(apply(3, ++timestamp, session, MapCommands.Put.builder().withKey("foo").withValue("Hello world again!").build()), "Hello world!");
    assertCleaned(2);
  }

  /**
   * Tests removing a value from a map.
   */
  public void testMapPutRemove() throws Throwable {
    long timestamp = System.currentTimeMillis();
    Session session = register(1, timestamp);
    assertNull(apply(2, ++timestamp, session, MapCommands.Put.builder().withKey("foo").withValue("Hello world!").build()));
    assertEquals(apply(++timestamp, session, MapCommands.Get.builder().withKey("foo").build()), "Hello world!");
    assertEquals(apply(3, ++timestamp, session, MapCommands.Remove.builder().withKey("foo").build()), "Hello world!");
    assertNull(apply(++timestamp, session, MapCommands.Get.builder().withKey("foo").build()));
    assertCleaned(2);
    assertNotCleaned(3);
  }

  /**
   * Tests that a remove commit is cleaned when no state is removed.
   */
  public void testMapRemoveClean() throws Throwable {
    long timestamp = System.currentTimeMillis();
    Session session = register(1, timestamp);
    assertNull(apply(2, ++timestamp, session, MapCommands.Remove.builder().withKey("foo").build()));
    assertCleaned(2);
  }

  /**
   * Tests putting and getting an ephemeral key.
   */
  public void testMapEphemeralPutGet() throws Throwable {
    long timestamp = System.currentTimeMillis();
    Session session1 = register(1, timestamp);
    Session session2 = register(2, timestamp);
    assertNull(apply(3, ++timestamp, session1, MapCommands.Put.builder().withKey("foo").withValue("Hello world!").withPersistence(PersistenceMode.EPHEMERAL).build()));
    assertEquals(apply(++timestamp, session2, MapCommands.Get.builder().withKey("foo").build()), "Hello world!");
    keepAlive(4, timestamp + 1000, session2);
    assertTrue(session1.isExpired());
    assertNull(apply(timestamp + 1001, session2, MapCommands.Get.builder().withKey("foo").build()));
  }

  /**
   * Tests putting and getting a TTL key.
   */
  public void testMapTtlPutGet() throws Throwable {
    long timestamp = System.currentTimeMillis();
    Session session = register(1, timestamp);
    assertNull(apply(2, ++timestamp, session, MapCommands.Put.builder().withKey("foo").withValue("Hello world!").withTtl(1000).build()));
    assertEquals(apply(++timestamp, session, MapCommands.Get.builder().withKey("foo").build()), "Hello world!");
    keepAlive(4, timestamp + 2000, session);
    assertNull(apply(timestamp + 2001, session, MapCommands.Get.builder().withKey("foo").build()));
  }

}
