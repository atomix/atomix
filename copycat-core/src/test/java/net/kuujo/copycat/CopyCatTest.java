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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.registry.Registry;
import net.kuujo.copycat.registry.impl.ConcurrentRegistry;

import org.junit.Assert;
import org.junit.Test;

/**
 * CopyCat test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopyCatTest {

  @Test
  public void testCopyCat() throws Exception {
    new RunnableTest() {
      @Override
      public void run() throws Exception {
        Set<CopyCatContext> contexts = startCluster(3);
        final CopyCatContext context = contexts.iterator().next();
        context.submitCommand("set", "foo", "bar").thenRun(() -> {
          context.submitCommand("get", "foo").whenComplete((result, error) -> {
            Assert.assertNull(error);
            Assert.assertEquals("bar", result);
            testComplete();
          });
        });
      }
    }.start();
  }

  /**
   * Starts a cluster of uniquely named CopyCat contexts.
   */
  private Set<CopyCatContext> startCluster(int numInstances) throws InterruptedException {
    final CountDownLatch latch = new CountDownLatch(3);
    Set<CopyCatContext> contexts = createCluster(3);
    for (CopyCatContext context : contexts) {
      context.start().whenComplete((result, error) -> {
        Assert.assertNull(error);
        latch.countDown();
      });
    }
    latch.await(10, TimeUnit.SECONDS);
    return contexts;
  }

  /**
   * Creates a cluster of uniquely named CopyCat contexts.
   */
  private Set<CopyCatContext> createCluster(int numInstances) {
    Registry registry = new ConcurrentRegistry();
    Set<CopyCatContext> instances = new HashSet<>();
    for (int i = 1; i <= numInstances; i++) {
      ClusterConfig cluster = new ClusterConfig();
      cluster.setLocalMember(String.format("local:%d", i));
      for (int j = 1; j <= numInstances; j++) {
        if (j != i) {
          cluster.addRemoteMember(String.format("local:%d", j));
        }
      }
      instances.add(new CopyCatContext(new TestStateMachine(), cluster, new CopyCatConfig().withMaxLogSize(10), registry));
    }
    return instances;
  }

  private static class TestStateMachine implements StateMachine {
    @Stateful
    private final Map<String, Object> data = new HashMap<>();

    @Command(type=Command.Type.WRITE)
    public void set(String key, Object value) {
      data.put(key, value);
    }

    @Command(type=Command.Type.READ)
    public Object get(String key) {
      return data.get(key);
    }

    @Command(type=Command.Type.WRITE)
    public void delete(String key) {
      data.remove(key);
    }

    @Command(type=Command.Type.WRITE)
    public void clear() {
      data.clear();
    }

  }

  /**
   * Runnable test.
   */
  private static abstract class RunnableTest {
    private final CountDownLatch latch = new CountDownLatch(1);;
    private final long timeout;
    private final TimeUnit unit;

    private RunnableTest() {
      this(30, TimeUnit.SECONDS);
    }

    private RunnableTest(long timeout, TimeUnit unit) {
      this.timeout = timeout;
      this.unit = unit;
    }

    /**
     * Starts the test.
     */
    public void start() throws Exception {
      run();
      latch.await(timeout, unit);
    }

    /**
     * Runs the test.
     */
    public abstract void run() throws Exception;

    /**
     * Completes the test.
     */
    protected void testComplete() {
      latch.countDown();
    }
  }

}
