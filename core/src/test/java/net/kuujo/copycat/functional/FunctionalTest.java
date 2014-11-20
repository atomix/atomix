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
package net.kuujo.copycat.functional;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.jodah.concurrentunit.ConcurrentTestCase;
import net.jodah.concurrentunit.Waiter;
import net.kuujo.copycat.AsyncCopycat;
import net.kuujo.copycat.Command;
import net.kuujo.copycat.Query;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.LocalClusterConfig;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.log.InMemoryLog;
import net.kuujo.copycat.protocol.AsyncLocalProtocol;

import org.testng.annotations.Test;

/**
 * Copycat test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public abstract class FunctionalTest extends ConcurrentTestCase {
  /**
   * Starts a cluster of contexts.
   */
  protected void startCluster(Set<AsyncCopycat> copycats) throws Throwable {
    Waiter waiter = new Waiter();
    waiter.expectResumes(copycats.size());
    for (AsyncCopycat copycat : copycats) {
      copycat.start().whenComplete((result, error) -> {
        waiter.assertNull(error);
        waiter.resume();
      });
    }

    waiter.await(10000);
  }

  protected void stopCluster(Set<AsyncCopycat> copycats) throws Throwable {
    Waiter waiter = new Waiter();
    waiter.expectResumes(copycats.size());
    for (AsyncCopycat copycat : copycats) {
      copycat.stop().whenComplete((v, error) -> {
        waiter.assertNull(error);
        waiter.resume();
      });
    }

    waiter.await(5000);
  }

  /**
   * Starts a cluster of uniquely named CopyCat contexts.
   */
  protected Set<AsyncCopycat> startCluster(int numInstances) throws Throwable {
    Set<AsyncCopycat> contexts = createCluster(numInstances);
    startCluster(contexts);
    return contexts;
  }

  /**
   * Creates a cluster of uniquely named CopyCat contexts.
   */
  protected Set<AsyncCopycat> createCluster(int numInstances) {
    AsyncLocalProtocol protocol = new AsyncLocalProtocol();
    Set<AsyncCopycat> instances = new HashSet<>(numInstances);
    for (int i = 1; i <= numInstances; i++) {
      LocalClusterConfig config = new LocalClusterConfig();
      config.setLocalMember(String.valueOf(i));
      for (int j = 1; j <= numInstances; j++) {
        if (j != i) {
          config.addRemoteMember(String.valueOf(j));
        }
      }

      instances.add(AsyncCopycat.builder()
          .withStateMachine(new TestStateMachine())
          .withLog(new InMemoryLog())
          .withCluster(new Cluster<Member>(config))
          .withProtocol(protocol)
          .build());
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
