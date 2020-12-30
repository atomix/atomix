/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.core.multimap.impl;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.atomix.core.multimap.AtomicMultimapType;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.service.ServiceContext;
import io.atomix.primitive.service.impl.DefaultBackupInput;
import io.atomix.primitive.service.impl.DefaultBackupOutput;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.HeapBuffer;
import io.atomix.utils.time.Versioned;
import io.atomix.utils.time.WallClock;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Consistent set multimap service test.
 */
public class DefaultAtomicMultimapServiceTest {
  private final String keyOne = "hello";
  private final String keyTwo = "goodbye";
  private final String keyThree = "foo";
  private final String keyFour = "bar";
  private final byte[] valueOne = keyOne.getBytes(Charsets.UTF_8);
  private final byte[] valueTwo = keyTwo.getBytes(Charsets.UTF_8);
  private final byte[] valueThree = keyThree.getBytes(Charsets.UTF_8);
  private final byte[] valueFour = keyFour.getBytes(Charsets.UTF_8);
  private final List<String> allKeys = Lists.newArrayList(keyOne, keyTwo,
          keyThree, keyFour);
  private final List<byte[]> allValues = Lists.newArrayList(valueOne, valueTwo,
          valueThree, valueFour);

  @Test
  @SuppressWarnings("unchecked")
  public void testSnapshot() throws Exception {
    ServiceContext context = mock(ServiceContext.class);
    when(context.serviceType()).thenReturn(AtomicMultimapType.instance());
    when(context.serviceName()).thenReturn("test");
    when(context.serviceId()).thenReturn(PrimitiveId.from(1));
    when(context.wallClock()).thenReturn(new WallClock());

    Session session = mock(Session.class);
    when(session.sessionId()).thenReturn(SessionId.from(1));

    DefaultAtomicMultimapService service = new DefaultAtomicMultimapService();
    service.init(context);
    service.put("foo", "Hello world!".getBytes());

    Buffer buffer = HeapBuffer.allocate();
    service.backup(new DefaultBackupOutput(buffer, service.serializer()));

    service = new DefaultAtomicMultimapService();
    service.init(context);
    service.restore(new DefaultBackupInput(buffer.flip(), service.serializer()));

    Versioned<Collection<byte[]>> value = service.get("foo");
    assertNotNull(value);
    assertEquals(1, value.value().size());
    assertArrayEquals("Hello world!".getBytes(), value.value().iterator().next());
  }

  /**
   * Contains tests for putAll and removeAll.
   * @throws Exception
   */
  @Test
  public void multiPutAllAndMultiRemoveAllTest() throws Exception {
    ServiceContext context = mock(ServiceContext.class);
    when(context.serviceType()).thenReturn(AtomicMultimapType.instance());
    when(context.serviceName()).thenReturn("test");
    when(context.serviceId()).thenReturn(PrimitiveId.from(1));
    when(context.wallClock()).thenReturn(new WallClock());

    Session session = mock(Session.class);
    when(session.sessionId()).thenReturn(SessionId.from(1));

    DefaultAtomicMultimapService service = new DefaultAtomicMultimapService();
    service.init(context);
    // Test multi put
    Map<String, Collection<? extends byte[]>> mapping = Maps.newHashMap();
    // First build the mappings having each key a different mapping
    allKeys.forEach(key -> {
      switch (key) {
        case keyOne:
          mapping.put(key, Lists.newArrayList(allValues.subList(0, 1)));
          break;
        case keyTwo:
          mapping.put(key, Lists.newArrayList(allValues.subList(0, 2)));
          break;
        case keyThree:
          mapping.put(key, Lists.newArrayList(allValues.subList(0, 3)));
          break;
        default:
          mapping.put(key, Lists.newArrayList(allValues.subList(0, 4)));
          break;
      }
    });
    assertEquals(0, service.size());
    assertTrue(service.putAll(mapping));
    // Keys are already present operation has to fail
    assertFalse(service.putAll(mapping));
    assertEquals(10, service.size());
    // Verify mapping is ok
    allKeys.forEach(key -> {
      Versioned<Collection<byte[]>> result = service.get(key);
      switch (key) {
        case keyOne:
          assertTrue(byteArrayCollectionIsEqual(allValues.subList(0, 1), result.value()));
          break;
        case keyTwo:
          assertTrue(byteArrayCollectionIsEqual(allValues.subList(0, 2), result.value()));
          break;
        case keyThree:
          assertTrue(byteArrayCollectionIsEqual(allValues.subList(0, 3), result.value()));
          break;
        default:
          assertTrue(byteArrayCollectionIsEqual(allValues.subList(0, 4), result.value()));
          break;
      }
    });
    assertTrue(service.removeAll(mapping));
    assertFalse(service.removeAll(mapping));
    // No more elements
    assertEquals(0, service.size());
  }

  /**
   * Returns two arrays contain the same set of elements,
   * regardless of order.
   * @param o1 first collection
   * @param o2 second collection
   * @return true if they contain the same elements
   */
  private boolean byteArrayCollectionIsEqual(
          Collection<? extends byte[]> o1, Collection<? extends byte[]> o2) {
    if (o1 == null || o2 == null || o1.size() != o2.size()) {
      return false;
    }
    for (byte[] array1 : o1) {
      boolean matched = false;
      for (byte[] array2 : o2) {
        if (Arrays.equals(array1, array2)) {
          matched = true;
          break;
        }
      }
      if (!matched) {
        return false;
      }
    }
    return true;
  }

}
