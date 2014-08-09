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
package net.kuujo.copycat.netty;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.kuujo.copycat.AnnotatedStateMachine;
import net.kuujo.copycat.Arguments;
import net.kuujo.copycat.AsyncCallback;
import net.kuujo.copycat.AsyncResult;
import net.kuujo.copycat.Command;
import net.kuujo.copycat.CopyCatConfig;
import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.Stateful;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.impl.DynamicClusterConfig;
import net.kuujo.copycat.registry.Registry;
import net.kuujo.copycat.registry.impl.ConcurrentRegistry;

import org.junit.Assert;
import org.junit.Test;

/**
 * CopyCat test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NettyTest {

  @Test
  public void testCopyCat() throws Exception {
    new RunnableTest() {
      @Override
      public void run() throws Exception {
        Set<CopyCatContext> contexts = startCluster(3);
        Arguments args = new Arguments().put("key", "foo").put("value", "bar");
        final CopyCatContext context = contexts.iterator().next();
        context.submitCommand("set", args, new AsyncCallback<Void>() {
          @Override
          public void call(AsyncResult<Void> result) {
            Assert.assertTrue(result.succeeded());
            Arguments args = new Arguments().put("key", "foo");
            context.submitCommand("get", args, new AsyncCallback<String>() {
              @Override
              public void call(AsyncResult<String> result) {
                Assert.assertTrue(result.succeeded());
                Assert.assertEquals("bar", result.value());
                testComplete();
              }
            });
          }
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
      context.start(new AsyncCallback<String>() {
        @Override
        public void call(AsyncResult<String> result) {
          Assert.assertTrue(result.succeeded());
          latch.countDown();
        }
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
      ClusterConfig cluster = new DynamicClusterConfig();
      cluster.setLocalMember(String.format("tcp://localhost:%d", i+50505));
      for (int j = 1; j <= numInstances; j++) {
        if (j != i) {
          cluster.addRemoteMember(String.format("tcp://localhost:%d", j+50505));
        }
      }
      instances.add(new CopyCatContext(new TestStateMachine(), cluster, new CopyCatConfig().setMaxLogSize(10), registry));
    }
    return instances;
  }

  private static class TestStateMachine extends AnnotatedStateMachine {
    @Stateful
    private final Map<String, Object> data = new HashMap<>();

    @Command(name="set", type=Command.Type.WRITE)
    public void set(@Command.Argument("key") String key, @Command.Argument("value") Object value) {
      data.put(key, value);
    }

    @Command(name="get", type=Command.Type.READ)
    public Object get(@Command.Argument("key") String key) {
      return data.get(key);
    }

    @Command(name="delete", type=Command.Type.WRITE)
    public void delete(@Command.Argument("key") String key) {
      data.remove(key);
    }

    @Command(name="clear", type=Command.Type.WRITE)
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
