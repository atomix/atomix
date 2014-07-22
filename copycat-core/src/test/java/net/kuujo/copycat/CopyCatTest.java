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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.impl.DynamicClusterConfig;
import net.kuujo.copycat.registry.Registry;
import net.kuujo.copycat.registry.impl.ConcurrentRegistry;
import net.kuujo.copycat.util.AsyncCallback;

import org.junit.Assert;
import org.junit.Test;

/**
 * CopyCat test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopyCatTest {

  @Test
  public void testCopyCat() {
    final CountDownLatch latch = new CountDownLatch(3);
    Set<CopyCatContext> contexts = createContexts(3);
    for (CopyCatContext context : contexts) {
      context.start(new AsyncCallback<String>() {
        @Override
        public void complete(String leader) {
          latch.countDown();
        }
        @Override
        public void fail(Throwable t) {
          latch.countDown();
        }
      });
    }
    try {
      latch.await(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Assert.fail(e.getMessage());
    }
    while (true) {
      
    }
  }

  private Set<CopyCatContext> createContexts(int numInstances) {
    Registry registry = new ConcurrentRegistry();
    Set<CopyCatContext> instances = new HashSet<>();
    for (int i = 1; i <= numInstances; i++) {
      ClusterConfig cluster = new DynamicClusterConfig();
      cluster.setLocalMember(String.format("direct:%d", i));
      for (int j = 1; j <= numInstances; j++) {
        if (j != i) {
          cluster.addRemoteMember(String.format("direct:%d", j));
        }
      }
      instances.add(new CopyCatContext(new TestStateMachine(), cluster, registry));
    }
    return instances;
  }

  private static class TestStateMachine implements StateMachine, CommandProvider {
    @Override
    public CommandInfo getCommandInfo(String name) {
      return new GenericCommandInfo(name, CommandInfo.Type.READ_WRITE);
    }

    @Override
    public Map<String, Object> createSnapshot() {
      return null;
    }

    @Override
    public void installSnapshot(Map<String, Object> snapshot) {
      
    }

    @Override
    public Map<String, Object> applyCommand(String name, Map<String, Object> args) {
      return null;
    }
  }

}
