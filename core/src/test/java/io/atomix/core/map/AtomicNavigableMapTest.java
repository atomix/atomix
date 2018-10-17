/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.map;

import com.google.common.collect.Sets;
import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.core.map.impl.AtomicNavigableMapProxy;
import org.junit.Test;

import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link AtomicNavigableMapProxy}.
 */
public class AtomicNavigableMapTest extends AbstractPrimitiveTest {
  @Test
  public void testSubMaps() throws Throwable {
    AtomicNavigableMap<String, String> map = createResource("testSubMaps").sync();

    for (char letter = 'a'; letter <= 'z'; letter++) {
      map.put(String.valueOf(letter), String.valueOf(letter));
    }

    assertEquals("a", map.firstKey());
    assertTrue(map.navigableKeySet().remove("a"));
    assertEquals("b", map.firstKey());
    assertTrue("b", map.navigableKeySet().descendingSet().remove("b"));
    assertEquals("c", map.firstKey());
    assertEquals("c", map.firstEntry().getValue().value());

    assertEquals("z", map.lastKey());
    assertTrue(map.navigableKeySet().remove("z"));
    assertEquals("y", map.lastKey());
    assertTrue(map.navigableKeySet().descendingSet().remove("y"));
    assertEquals("x", map.lastKey());
    assertEquals("x", map.lastEntry().getValue().value());

    assertEquals("d", map.subMap("d", true, "w", false).firstKey());
    assertEquals("d", map.subMap("d", true, "w", false).firstEntry().getValue().value());
    assertEquals("e", map.subMap("d", false, "w", false).firstKey());
    assertEquals("e", map.subMap("d", false, "w", false).firstEntry().getValue().value());
    assertEquals("d", map.tailMap("d", true).firstKey());
    assertEquals("d", map.tailMap("d", true).firstEntry().getValue().value());
    assertEquals("e", map.tailMap("d", false).firstKey());
    assertEquals("e", map.tailMap("d", false).firstEntry().getValue().value());
    assertEquals("w", map.headMap("w", true).navigableKeySet().descendingSet().first());
    assertEquals("v", map.headMap("w", false).navigableKeySet().descendingSet().first());

    assertEquals("w", map.subMap("d", false, "w", true).lastKey());
    assertEquals("w", map.subMap("d", false, "w", true).lastEntry().getValue().value());
    assertEquals("v", map.subMap("d", false, "w", false).lastKey());
    assertEquals("v", map.subMap("d", false, "w", false).lastEntry().getValue().value());
    assertEquals("w", map.headMap("w", true).lastKey());
    assertEquals("w", map.headMap("w", true).lastEntry().getValue().value());
    assertEquals("v", map.headMap("w", false).lastKey());
    assertEquals("v", map.headMap("w", false).lastEntry().getValue().value());
    assertEquals("d", map.tailMap("d", true).navigableKeySet().descendingSet().last());
    assertEquals("d", map.tailMap("d", true).navigableKeySet().descendingSet().last());
    assertEquals("e", map.tailMap("d", false).navigableKeySet().descendingSet().last());
    assertEquals("e", map.tailMap("d", false).navigableKeySet().descendingSet().last());

    assertEquals("w", map.subMap("d", false, "w", true).navigableKeySet().descendingSet().first());
    assertEquals("v", map.subMap("d", false, "w", false).navigableKeySet().descendingSet().first());

    assertEquals(20, map.subMap("d", true, "w", true).size());
    assertEquals(19, map.subMap("d", true, "w", false).size());
    assertEquals(19, map.subMap("d", false, "w", true).size());
    assertEquals(18, map.subMap("d", false, "w", false).size());

    assertEquals(20, map.subMap("d", true, "w", true).entrySet().stream().count());
    assertEquals(19, map.subMap("d", true, "w", false).entrySet().stream().count());
    assertEquals(19, map.subMap("d", false, "w", true).entrySet().stream().count());
    assertEquals(18, map.subMap("d", false, "w", false).entrySet().stream().count());

    assertEquals("d", map.subMap("d", true, "w", true).entrySet().stream().findFirst().get().getValue().value());
    assertEquals("d", map.subMap("d", true, "w", false).entrySet().stream().findFirst().get().getValue().value());
    assertEquals("e", map.subMap("d", false, "w", true).entrySet().stream().findFirst().get().getValue().value());
    assertEquals("e", map.subMap("d", false, "w", false).entrySet().stream().findFirst().get().getValue().value());

    assertEquals("w", map.subMap("d", true, "w", true).navigableKeySet().descendingSet().stream().findFirst().get());
    assertEquals("v", map.subMap("d", true, "w", false).navigableKeySet().descendingSet().stream().findFirst().get());
    assertEquals("w", map.subMap("d", false, "w", true).navigableKeySet().descendingSet().stream().findFirst().get());
    assertEquals("v", map.subMap("d", false, "w", false).navigableKeySet().descendingSet().stream().findFirst().get());

    assertEquals("d", map.subMap("d", true, "w", true).entrySet().iterator().next().getKey());
    assertEquals("w", map.subMap("d", true, "w", true).navigableKeySet().descendingIterator().next());
    assertEquals("w", map.subMap("d", true, "w", true).navigableKeySet().descendingSet().iterator().next());

    assertEquals("e", map.subMap("d", false, "w", true).entrySet().iterator().next().getKey());
    assertEquals("e", map.subMap("d", false, "w", true).navigableKeySet().descendingSet().descendingIterator().next());
    assertEquals("w", map.subMap("d", false, "w", true).navigableKeySet().descendingIterator().next());
    assertEquals("w", map.subMap("d", false, "w", true).navigableKeySet().descendingSet().iterator().next());

    assertEquals("d", map.subMap("d", true, "w", false).entrySet().iterator().next().getKey());
    assertEquals("d", map.subMap("d", true, "w", false).navigableKeySet().descendingSet().descendingIterator().next());
    assertEquals("v", map.subMap("d", true, "w", false).navigableKeySet().descendingIterator().next());
    assertEquals("v", map.subMap("d", true, "w", false).navigableKeySet().descendingSet().iterator().next());

    assertEquals("e", map.subMap("d", false, "w", false).entrySet().iterator().next().getKey());
    assertEquals("e", map.subMap("d", false, "w", false).navigableKeySet().descendingSet().descendingIterator().next());
    assertEquals("v", map.subMap("d", false, "w", false).navigableKeySet().descendingIterator().next());
    assertEquals("v", map.subMap("d", false, "w", false).navigableKeySet().descendingSet().iterator().next());

    assertEquals("d", map.subMap("d", true, "w", true).navigableKeySet().headSet("m", true).iterator().next());
    assertEquals("m", map.subMap("d", true, "w", true).navigableKeySet().headSet("m", true).descendingIterator().next());
    assertEquals("d", map.subMap("d", true, "w", true).navigableKeySet().headSet("m", false).iterator().next());
    assertEquals("l", map.subMap("d", true, "w", true).navigableKeySet().headSet("m", false).descendingIterator().next());

    assertEquals("m", map.subMap("d", true, "w", true).navigableKeySet().tailSet("m", true).iterator().next());
    assertEquals("w", map.subMap("d", true, "w", true).navigableKeySet().tailSet("m", true).descendingIterator().next());
    assertEquals("n", map.subMap("d", true, "w", true).navigableKeySet().tailSet("m", false).iterator().next());
    assertEquals("w", map.subMap("d", true, "w", true).navigableKeySet().tailSet("m", false).descendingIterator().next());

    assertEquals(18, map.subMap("d", true, "w", true)
        .subMap("e", true, "v", true)
        .subMap("d", true, "w", true)
        .size());

    assertEquals("x", map.tailMap("d", true).navigableKeySet().descendingIterator().next());
    assertEquals("x", map.tailMap("d", true).navigableKeySet().descendingSet().iterator().next());
    assertEquals("c", map.headMap("w", true).navigableKeySet().iterator().next());
    assertEquals("c", map.headMap("w", true).navigableKeySet().descendingSet().descendingSet().iterator().next());

    map.headMap("e", false).clear();
    assertEquals("e", map.navigableKeySet().first());
    assertEquals(20, map.navigableKeySet().size());

    map.headMap("g", true).clear();
    assertEquals("h", map.navigableKeySet().first());
    assertEquals(17, map.navigableKeySet().size());

    map.tailMap("t", false).clear();
    assertEquals("t", map.navigableKeySet().last());
    assertEquals(13, map.navigableKeySet().size());

    map.tailMap("o", true).clear();
    assertEquals("n", map.navigableKeySet().last());
    assertEquals(7, map.navigableKeySet().size());

    map.navigableKeySet().subSet("k", false, "n", false).clear();
    assertEquals(5, map.navigableKeySet().size());
    assertEquals(Sets.newHashSet("h", "i", "j", "k", "n"), Sets.newHashSet(map.navigableKeySet()));
  }

  @Test
  public void testKeySetOperations() throws Throwable {
    AtomicNavigableMap<String, String> map = createResource("testKeySetOperations").sync();

    try {
      map.navigableKeySet().first();
      fail();
    } catch (NoSuchElementException e) {
    }

    try {
      map.navigableKeySet().last();
      fail();
    } catch (NoSuchElementException e) {
    }

    try {
      map.navigableKeySet().subSet("a", false, "z", false).first();
      fail();
    } catch (NoSuchElementException e) {
    }

    try {
      map.navigableKeySet().subSet("a", false, "z", false).last();
      fail();
    } catch (NoSuchElementException e) {
    }

    assertEquals(0, map.navigableKeySet().size());
    assertTrue(map.navigableKeySet().isEmpty());
    assertEquals(0, map.navigableKeySet().subSet("a", true, "b", true).size());
    assertTrue(map.navigableKeySet().subSet("a", true, "b", true).isEmpty());
    assertEquals(0, map.navigableKeySet().headSet("a").size());
    assertTrue(map.navigableKeySet().headSet("a").isEmpty());
    assertEquals(0, map.navigableKeySet().tailSet("b").size());
    assertTrue(map.navigableKeySet().tailSet("b").isEmpty());

    for (char letter = 'a'; letter <= 'z'; letter++) {
      map.put(String.valueOf(letter), String.valueOf(letter));
    }

    assertEquals("a", map.navigableKeySet().first());
    assertEquals("z", map.navigableKeySet().last());
    assertTrue(map.navigableKeySet().remove("a"));
    assertTrue(map.navigableKeySet().remove("z"));
    assertEquals("b", map.navigableKeySet().first());
    assertEquals("y", map.navigableKeySet().last());

    try {
      map.navigableKeySet().subSet("A", false, "Z", false).first();
      fail();
    } catch (NoSuchElementException e) {
    }

    try {
      map.navigableKeySet().subSet("A", false, "Z", false).last();
      fail();
    } catch (NoSuchElementException e) {
    }

    try {
      map.navigableKeySet().subSet("a", true, "b", false).first();
      fail();
    } catch (NoSuchElementException e) {
    }

    try {
      map.navigableKeySet().subSet("a", true, "b", false).last();
      fail();
    } catch (NoSuchElementException e) {
    }

    assertEquals("d", map.navigableKeySet().subSet("c", false, "x", false)
        .subSet("c", true, "x", true).first());
    assertEquals("w", map.navigableKeySet().subSet("c", false, "x", false)
        .subSet("c", true, "x", true).last());

    assertEquals("y", map.navigableKeySet().headSet("y", true).last());
    assertEquals("x", map.navigableKeySet().headSet("y", false).last());
    assertEquals("y", map.navigableKeySet().headSet("y", true)
        .subSet("a", true, "z", false).last());

    assertEquals("b", map.navigableKeySet().tailSet("b", true).first());
    assertEquals("c", map.navigableKeySet().tailSet("b", false).first());
    assertEquals("b", map.navigableKeySet().tailSet("b", true)
        .subSet("a", false, "z", true).first());

    assertEquals("b", map.navigableKeySet().higher("a"));
    assertEquals("c", map.navigableKeySet().higher("b"));
    assertEquals("y", map.navigableKeySet().lower("z"));
    assertEquals("x", map.navigableKeySet().lower("y"));

    assertEquals("b", map.navigableKeySet().ceiling("a"));
    assertEquals("b", map.navigableKeySet().ceiling("b"));
    assertEquals("y", map.navigableKeySet().floor("z"));
    assertEquals("y", map.navigableKeySet().floor("y"));

    assertEquals("c", map.navigableKeySet().subSet("c", true, "x", true).higher("b"));
    assertEquals("d", map.navigableKeySet().subSet("c", true, "x", true).higher("c"));
    assertEquals("x", map.navigableKeySet().subSet("c", true, "x", true).lower("y"));
    assertEquals("w", map.navigableKeySet().subSet("c", true, "x", true).lower("x"));

    assertEquals("d", map.navigableKeySet().subSet("c", false, "x", false).higher("b"));
    assertEquals("d", map.navigableKeySet().subSet("c", false, "x", false).higher("c"));
    assertEquals("e", map.navigableKeySet().subSet("c", false, "x", false).higher("d"));
    assertEquals("w", map.navigableKeySet().subSet("c", false, "x", false).lower("y"));
    assertEquals("w", map.navigableKeySet().subSet("c", false, "x", false).lower("x"));
    assertEquals("v", map.navigableKeySet().subSet("c", false, "x", false).lower("w"));

    assertEquals("c", map.navigableKeySet().subSet("c", true, "x", true).ceiling("b"));
    assertEquals("c", map.navigableKeySet().subSet("c", true, "x", true).ceiling("c"));
    assertEquals("x", map.navigableKeySet().subSet("c", true, "x", true).floor("y"));
    assertEquals("x", map.navigableKeySet().subSet("c", true, "x", true).floor("x"));

    assertEquals("d", map.navigableKeySet().subSet("c", false, "x", false).ceiling("b"));
    assertEquals("d", map.navigableKeySet().subSet("c", false, "x", false).ceiling("c"));
    assertEquals("d", map.navigableKeySet().subSet("c", false, "x", false).ceiling("d"));
    assertEquals("w", map.navigableKeySet().subSet("c", false, "x", false).floor("y"));
    assertEquals("w", map.navigableKeySet().subSet("c", false, "x", false).floor("x"));
    assertEquals("w", map.navigableKeySet().subSet("c", false, "x", false).floor("w"));
  }

  @Test
  public void testKeySetSubSets() throws Throwable {
    AtomicNavigableMap<String, String> map = createResource("testKeySetSubSets").sync();

    for (char letter = 'a'; letter <= 'z'; letter++) {
      map.put(String.valueOf(letter), String.valueOf(letter));
    }

    assertEquals("a", map.navigableKeySet().first());
    assertTrue(map.navigableKeySet().remove("a"));
    assertEquals("b", map.navigableKeySet().descendingSet().last());
    assertTrue("b", map.navigableKeySet().descendingSet().remove("b"));

    assertEquals("z", map.navigableKeySet().last());
    assertTrue(map.navigableKeySet().remove("z"));
    assertEquals("y", map.navigableKeySet().descendingSet().first());
    assertTrue(map.navigableKeySet().descendingSet().remove("y"));

    assertEquals("d", map.navigableKeySet().subSet("d", true, "w", false).first());
    assertEquals("e", map.navigableKeySet().subSet("d", false, "w", false).first());
    assertEquals("d", map.navigableKeySet().tailSet("d", true).first());
    assertEquals("e", map.navigableKeySet().tailSet("d", false).first());
    assertEquals("w", map.navigableKeySet().headSet("w", true).descendingSet().first());
    assertEquals("v", map.navigableKeySet().headSet("w", false).descendingSet().first());

    assertEquals("w", map.navigableKeySet().subSet("d", false, "w", true).last());
    assertEquals("v", map.navigableKeySet().subSet("d", false, "w", false).last());
    assertEquals("w", map.navigableKeySet().headSet("w", true).last());
    assertEquals("v", map.navigableKeySet().headSet("w", false).last());
    assertEquals("d", map.navigableKeySet().tailSet("d", true).descendingSet().last());
    assertEquals("e", map.navigableKeySet().tailSet("d", false).descendingSet().last());

    assertEquals("w", map.navigableKeySet().subSet("d", false, "w", true).descendingSet().first());
    assertEquals("v", map.navigableKeySet().subSet("d", false, "w", false).descendingSet().first());

    assertEquals(20, map.navigableKeySet().subSet("d", true, "w", true).size());
    assertEquals(19, map.navigableKeySet().subSet("d", true, "w", false).size());
    assertEquals(19, map.navigableKeySet().subSet("d", false, "w", true).size());
    assertEquals(18, map.navigableKeySet().subSet("d", false, "w", false).size());

    assertEquals(20, map.navigableKeySet().subSet("d", true, "w", true).stream().count());
    assertEquals(19, map.navigableKeySet().subSet("d", true, "w", false).stream().count());
    assertEquals(19, map.navigableKeySet().subSet("d", false, "w", true).stream().count());
    assertEquals(18, map.navigableKeySet().subSet("d", false, "w", false).stream().count());

    assertEquals("d", map.navigableKeySet().subSet("d", true, "w", true).stream().findFirst().get());
    assertEquals("d", map.navigableKeySet().subSet("d", true, "w", false).stream().findFirst().get());
    assertEquals("e", map.navigableKeySet().subSet("d", false, "w", true).stream().findFirst().get());
    assertEquals("e", map.navigableKeySet().subSet("d", false, "w", false).stream().findFirst().get());

    assertEquals("w", map.navigableKeySet().subSet("d", true, "w", true).descendingSet().stream().findFirst().get());
    assertEquals("v", map.navigableKeySet().subSet("d", true, "w", false).descendingSet().stream().findFirst().get());
    assertEquals("w", map.navigableKeySet().subSet("d", false, "w", true).descendingSet().stream().findFirst().get());
    assertEquals("v", map.navigableKeySet().subSet("d", false, "w", false).descendingSet().stream().findFirst().get());

    assertEquals("d", map.navigableKeySet().subSet("d", true, "w", true).iterator().next());
    assertEquals("w", map.navigableKeySet().subSet("d", true, "w", true).descendingIterator().next());
    assertEquals("w", map.navigableKeySet().subSet("d", true, "w", true).descendingSet().iterator().next());

    assertEquals("e", map.navigableKeySet().subSet("d", false, "w", true).iterator().next());
    assertEquals("e", map.navigableKeySet().subSet("d", false, "w", true).descendingSet().descendingIterator().next());
    assertEquals("w", map.navigableKeySet().subSet("d", false, "w", true).descendingIterator().next());
    assertEquals("w", map.navigableKeySet().subSet("d", false, "w", true).descendingSet().iterator().next());

    assertEquals("d", map.navigableKeySet().subSet("d", true, "w", false).iterator().next());
    assertEquals("d", map.navigableKeySet().subSet("d", true, "w", false).descendingSet().descendingIterator().next());
    assertEquals("v", map.navigableKeySet().subSet("d", true, "w", false).descendingIterator().next());
    assertEquals("v", map.navigableKeySet().subSet("d", true, "w", false).descendingSet().iterator().next());

    assertEquals("e", map.navigableKeySet().subSet("d", false, "w", false).iterator().next());
    assertEquals("e", map.navigableKeySet().subSet("d", false, "w", false).descendingSet().descendingIterator().next());
    assertEquals("v", map.navigableKeySet().subSet("d", false, "w", false).descendingIterator().next());
    assertEquals("v", map.navigableKeySet().subSet("d", false, "w", false).descendingSet().iterator().next());

    assertEquals("d", map.navigableKeySet().subSet("d", true, "w", true).headSet("m", true).iterator().next());
    assertEquals("m", map.navigableKeySet().subSet("d", true, "w", true).headSet("m", true).descendingIterator().next());
    assertEquals("d", map.navigableKeySet().subSet("d", true, "w", true).headSet("m", false).iterator().next());
    assertEquals("l", map.navigableKeySet().subSet("d", true, "w", true).headSet("m", false).descendingIterator().next());

    assertEquals("m", map.navigableKeySet().subSet("d", true, "w", true).tailSet("m", true).iterator().next());
    assertEquals("w", map.navigableKeySet().subSet("d", true, "w", true).tailSet("m", true).descendingIterator().next());
    assertEquals("n", map.navigableKeySet().subSet("d", true, "w", true).tailSet("m", false).iterator().next());
    assertEquals("w", map.navigableKeySet().subSet("d", true, "w", true).tailSet("m", false).descendingIterator().next());

    assertEquals(18, map.navigableKeySet().subSet("d", true, "w", true)
        .subSet("e", true, "v", true)
        .subSet("d", true, "w", true)
        .size());

    assertEquals("x", map.navigableKeySet().tailSet("d", true).descendingIterator().next());
    assertEquals("x", map.navigableKeySet().tailSet("d", true).descendingSet().iterator().next());
    assertEquals("c", map.navigableKeySet().headSet("w", true).iterator().next());
    assertEquals("c", map.navigableKeySet().headSet("w", true).descendingSet().descendingSet().iterator().next());

    map.navigableKeySet().headSet("e", false).clear();
    assertEquals("e", map.navigableKeySet().first());
    assertEquals(20, map.navigableKeySet().size());

    map.navigableKeySet().headSet("g", true).clear();
    assertEquals("h", map.navigableKeySet().first());
    assertEquals(17, map.navigableKeySet().size());

    map.navigableKeySet().tailSet("t", false).clear();
    assertEquals("t", map.navigableKeySet().last());
    assertEquals(13, map.navigableKeySet().size());

    map.navigableKeySet().tailSet("o", true).clear();
    assertEquals("n", map.navigableKeySet().last());
    assertEquals(7, map.navigableKeySet().size());

    map.navigableKeySet().subSet("k", false, "n", false).clear();
    assertEquals(5, map.navigableKeySet().size());
    assertEquals(Sets.newHashSet("h", "i", "j", "k", "n"), Sets.newHashSet(map.navigableKeySet()));
  }

  private AsyncAtomicNavigableMap<String, String> createResource(String mapName) {
    try {
      return atomix().<String, String>atomicNavigableMapBuilder(mapName)
          .withProtocol(protocol())
          .build()
          .async();
    } catch (Throwable e) {
      throw new RuntimeException(e.toString());
    }
  }
}
