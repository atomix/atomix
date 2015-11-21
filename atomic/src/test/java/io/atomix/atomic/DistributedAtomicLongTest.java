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
package io.atomix.atomic;

import io.atomix.atomic.state.AtomicValueState;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.resource.ResourceStateMachine;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Distributed atomic long test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
@SuppressWarnings("unchecked")
public class DistributedAtomicLongTest extends AbstractAtomicTest {

  @Override
  protected ResourceStateMachine createStateMachine() {
    return new AtomicValueState();
  }

  /**
   * Tests setting and getting a value.
   */
  public void testAtomicIncrementAndGet() throws Throwable {
    testAtomic(3, atomic(DistributedAtomicLong::incrementAndGet, l -> l + 1));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testAtomicDecrementAndGet() throws Throwable {
    testAtomic(3, atomic(DistributedAtomicLong::decrementAndGet, l -> l - 1));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testAtomicGetAndIncrement() throws Throwable {
    testAtomic(3, atomic(DistributedAtomicLong::getAndIncrement, l -> l));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testAtomicGetAndDecrement() throws Throwable {
    testAtomic(3, atomic(DistributedAtomicLong::getAndDecrement, l -> l));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testAtomicAddAndGet() throws Throwable {
    testAtomic(3, atomic(l -> l.addAndGet(10), l -> l + 10));
  }

  /**
   * Tests setting and getting a value.
   */
  public void testAtomicGetAndAdd() throws Throwable {
    testAtomic(3, atomic(l -> l.getAndAdd(10), l -> l));
  }

  /**
   * Returns an atomic set/get test callback.
   */
  private Consumer<DistributedAtomicLong> atomic(Function<DistributedAtomicLong, CompletableFuture<Long>> commandFunction, Function<Long, Long> resultFunction) {
    return (a) -> {
      a.get().thenAccept(value -> {
        commandFunction.apply(a).thenAccept(result -> {
          threadAssertEquals(result, resultFunction.apply(value));
          resume();
        });
      });
    };
  }

  /**
   * Tests a set of atomic operations.
   */
  private void testAtomic(int servers, Consumer<DistributedAtomicLong> consumer) throws Throwable {
    createServers(servers);
    CopycatClient client = createClient();
    DistributedAtomicLong atomic = new DistributedAtomicLong(client);
    consumer.accept(atomic);
  }

}
