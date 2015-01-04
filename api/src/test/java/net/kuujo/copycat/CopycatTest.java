/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.collections.AsyncMap;
import net.kuujo.copycat.collections.AsyncMapConfig;
import net.kuujo.copycat.log.BufferedLog;
import net.kuujo.copycat.protocol.LocalProtocol;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Copycat test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class CopycatTest {

  /**
   * Test.
   */
  public void testCopycat() throws Exception {
    ClusterConfig cluster = new ClusterConfig()
      .withProtocol(new LocalProtocol())
      .withMembers("local://foo", "local://bar", "local://baz");

    CopycatConfig config = new CopycatConfig()
      .withClusterConfig(cluster)
      .addMapConfig("test", new AsyncMapConfig().withLog(new BufferedLog()));

    Copycat copycat1 = Copycat.create("local://foo", config);
    Copycat copycat2 = Copycat.create("local://bar", config);
    Copycat copycat3 = Copycat.create("local://baz", config);

    CountDownLatch latch = new CountDownLatch(3);

    copycat1.open().thenRun(latch::countDown);
    copycat2.open().thenRun(latch::countDown);
    copycat3.open().thenRun(latch::countDown);

    latch.await(30, TimeUnit.SECONDS);

    AsyncMap<String, String> map = copycat1.map("test");
    Assert.assertTrue(map.isEmpty().get());
    Assert.assertEquals(map.size().get(), Integer.valueOf(0));
    map.put("foo", "Hello world!").get();
    Assert.assertFalse(map.isEmpty().get());
    Assert.assertEquals(map.size().get(), Integer.valueOf(1));
    Assert.assertEquals(map.get("foo").get(), "Hello world!");
    map.remove("foo").get();
    Assert.assertTrue(map.isEmpty().get());
    Assert.assertEquals(map.size().get(), Integer.valueOf(0));
  }

}
