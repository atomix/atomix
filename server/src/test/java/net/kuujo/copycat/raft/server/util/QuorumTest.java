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
package net.kuujo.copycat.raft.server.util;

import net.kuujo.copycat.raft.util.Quorum;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;

/**
 * Quorum test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class QuorumTest {
  Quorum quorum;
  AtomicInteger callbackResult;

  @BeforeMethod
  protected void beforeMethod() {
    callbackResult = new AtomicInteger();
    quorum = new Quorum(3, r -> {
      callbackResult.set(r ? 1 : 2);
    });
  }

  /**
   * Tests a successful quorum.
   */
  public void testQuorumSucceed() {
    quorum.succeed();
    quorum.succeed();
    quorum.fail();
    assertEquals(callbackResult.get(), 0);
    quorum.fail();
    quorum.succeed();
    assertEquals(callbackResult.get(), 1);
  }

  /**
   * Tests a failed quorum.
   */
  public void testQuorumFail() {
    quorum.fail();
    quorum.fail();
    quorum.succeed();
    assertEquals(callbackResult.get(), 0);
    quorum.succeed();
    quorum.fail();
    assertEquals(callbackResult.get(), 2);
  }

}
