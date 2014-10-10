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

import net.jodah.concurrentunit.ConcurrentTestCase;
import net.jodah.concurrentunit.Waiter;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.LocalClusterConfig;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.log.MemoryMappedFileLog;
import net.kuujo.copycat.protocol.AsyncLocalProtocol;
import org.testng.annotations.Test;

import java.util.*;

/**
 * Copycat test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public abstract class AbstractCopycatTest extends ConcurrentTestCase {
  /**
   * Starts a cluster of contexts.
   */
  protected void startCluster(Set<AsyncCopycatContext> contexts) throws Throwable {
    Waiter waiter = new Waiter();
    for (AsyncCopycatContext context : contexts) {
      context.start().whenComplete((result, error) -> {
        waiter.assertNull(error);
        waiter.resume();
      });
    }
    
    waiter.await(10000, contexts.size());
  }

  /**
   * Starts a cluster of uniquely named CopyCat contexts.
   */
  protected Set<AsyncCopycatContext> startCluster(int numInstances) throws Throwable {
    Set<AsyncCopycatContext> contexts = createCluster(numInstances);
    startCluster(contexts);
    return contexts;
  }

  /**
   * Creates a cluster of uniquely named CopyCat contexts.
   */
  protected Set<AsyncCopycatContext> createCluster(int numInstances) {
    AsyncLocalProtocol protocol = new AsyncLocalProtocol();
    Set<AsyncCopycatContext> instances = new HashSet<>(numInstances);
    for (int i = 1; i <= numInstances; i++) {
      LocalClusterConfig config = new LocalClusterConfig();
      config.setLocalMember(String.valueOf(i));
      for (int j = 1; j <= numInstances; j++) {
        if (j != i) {
          config.addRemoteMember(String.valueOf(j));
        }
      }
      instances.add(AsyncCopycat.context(new TestStateMachine(), new MemoryMappedFileLog(String.format("target/test-logs/%s", UUID
        .randomUUID())), new Cluster<Member>(config), protocol));
    }
    return instances;
  }

  protected static class TestStateMachine implements StateMachine {
    private final Map<String, Object> data = new HashMap<>();

    @Override
    public byte[] takeSnapshot() {
      return new byte[0];
    }

    @Override
    public void installSnapshot(byte[] snapshot) {
      
    }

    @Command
    public void set(String key, Object value) {
      data.put(key, value);
    }

    @Query
    public Object get(String key) {
      return data.get(key);
    }

    @Command
    public void delete(String key) {
      data.remove(key);
    }

    @Command
    public void clear() {
      data.clear();
    }
  }
}
