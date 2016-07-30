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
package io.atomix.collections;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.testng.annotations.Test;

import io.atomix.testing.AbstractCopycatTest;

/**
 * Distributed map test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
@SuppressWarnings("unchecked")
public class DistributedSetTest extends AbstractCopycatTest<DistributedSet> {

  @Override
  protected Class<? super DistributedSet> type() {
    return DistributedSet.class;
  }

  /**
   * Tests adding and removing members from a set.
   */
  public void testSetAddRemove() throws Throwable {
    createServers(3);

    DistributedSet<String> set1 = createResource();
    assertFalse(set1.contains("Hello world!").get());

    DistributedSet<String> set2 = createResource();
    assertFalse(set2.contains("Hello world!").get());

    set1.add("Hello world!").join();
    assertTrue(set1.contains("Hello world!").get());
    assertTrue(set2.contains("Hello world!").get());

    set2.remove("Hello world!").join();
    assertFalse(set1.contains("Hello world!").get());
    assertFalse(set2.contains("Hello world!").get());
  }

  /**
   * Tests {@link DistributedSet#iterator()}.
   */
  public void testIterator() throws Throwable {
    createServers(3);

    DistributedSet<String> set = createResource();

    set.add("test1").thenRun(this::resume);
    set.add("test2").thenRun(this::resume);
    await(10, TimeUnit.SECONDS, 2);

    set.iterator().thenAccept(iterator -> {
      System.out.println(iterator);
      Iterable<String> iterable = () -> iterator;
      List<String> values = StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toList());
      System.out.println(values);
      threadAssertTrue(values.contains("test1"));
      threadAssertTrue(values.contains("test2"));
      resume();
    });
    await(10000);
  }
}
