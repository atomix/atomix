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
package io.atomix.core.map.impl;

import io.atomix.primitive.service.impl.DefaultBackupInput;
import io.atomix.primitive.service.impl.DefaultBackupOutput;
import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.HeapBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Atomic counter map service test.
 */
public class DefaultAtomicCounterMapServiceTest {
  @Test
  public void testSnapshot() throws Exception {
    DefaultAtomicCounterMapService service = new DefaultAtomicCounterMapService();
    service.put("foo", 1);

    Buffer buffer = HeapBuffer.allocate();
    service.backup(new DefaultBackupOutput(buffer, service.serializer()));

    service = new DefaultAtomicCounterMapService();
    service.restore(new DefaultBackupInput(buffer.flip(), service.serializer()));

    long value = service.get("foo");
    assertEquals(1, value);
  }
}
