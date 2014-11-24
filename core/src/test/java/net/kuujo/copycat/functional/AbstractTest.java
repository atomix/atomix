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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.jodah.concurrentunit.ConcurrentTestCase;
import net.jodah.concurrentunit.Waiter;
import net.kuujo.copycat.Command;
import net.kuujo.copycat.Copycat;
import net.kuujo.copycat.Query;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.log.InMemoryLog;
import net.kuujo.copycat.protocol.LocalProtocol;

import org.testng.annotations.Test;

/**
 * Functional test support.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public abstract class AbstractTest extends ConcurrentTestCase {
  /**
   * Starts a cluster of contexts.
   */
  protected void startCluster(Set<Copycat> copycats) throws Throwable {
    Waiter waiter = new Waiter();
    waiter.expectResumes(copycats.size());
    for (Copycat copycat : copycats) {
      copycat.start().whenComplete((result, error) -> {
        waiter.assertNull(error);
        waiter.resume();
      });
    }

    waiter.await(10000);
  }

  protected void stopCluster(Set<Copycat> copycats) throws Throwable {
    Waiter waiter = new Waiter();
    waiter.expectResumes(copycats.size());
    for (Copycat copycat : copycats) {
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
  protected Set<Copycat> startCluster(int numInstances) throws Throwable {
    Set<Copycat> contexts = createCluster(numInstances);
    startCluster(contexts);
    return contexts;
  }

  /**
   * Creates a cluster of uniquely named CopyCat contexts.
   */
  protected Set<Copycat> createCluster(int numInstances) {
    LocalProtocol protocol = new LocalProtocol();
    Set<Copycat> instances = new HashSet<>(numInstances);
    for (int i = 1; i <= numInstances; i++) {
      List<String> remoteEndpoints = new ArrayList<>();
      for (int j = 1; j <= numInstances; j++) {
        if (j != i) {
          remoteEndpoints.add(String.valueOf(j));
        }
      }

      Cluster cluster = new Cluster("localhost", remoteEndpoints);
      instances.add(new Copycat(new TestStateMachine(), new InMemoryLog(), cluster, protocol));
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
