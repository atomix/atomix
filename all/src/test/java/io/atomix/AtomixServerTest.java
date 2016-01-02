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
 * limitations under the License
 */
package io.atomix;

import java.util.List;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.atomix.testing.AbstractAtomixTest;

/**
 * Atomix server test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class AtomixServerTest extends AbstractAtomixTest {
  @BeforeMethod
  protected void beforeMethod() {
    init();
  }

  @AfterMethod
  protected void afterMethod() {
    cleanup();
  }

  /**
   * Tests joining a server to an existing cluster.
   */
  public void testServerJoin() throws Throwable {
    createServers(3);
    AtomixServer joiner = createServer(nextAddress(), members);
    joiner.open().thenRun(this::resume);
    await(30000);
  }

  /**
   * Tests leaving a sever from a cluster.
   */
  public void testServerLeave() throws Throwable {
    List<AtomixServer> servers = createServers(3);
    AtomixServer server = servers.get(0);
    server.close().thenRun(this::resume);
    await(30000);
  }

}
