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
package io.atomix.core.tree.impl;

import io.atomix.core.tree.DocumentPath;
import io.atomix.core.tree.impl.DocumentTreeService;
import io.atomix.core.tree.impl.DocumentTreeOperations.Get;
import io.atomix.core.tree.impl.DocumentTreeOperations.Update;
import io.atomix.primitive.Ordering;
import io.atomix.primitive.service.impl.DefaultCommit;
import io.atomix.primitive.session.Session;
import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.HeapBuffer;
import io.atomix.utils.time.Versioned;
import io.atomix.utils.Match;
import org.junit.Test;

import java.util.Optional;

import static io.atomix.core.tree.impl.DocumentTreeOperations.GET;
import static io.atomix.core.tree.impl.DocumentTreeOperations.UPDATE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

/**
 * Document tree service test.
 */
public class DocumentTreeServiceTest {

  @Test
  public void testNaturalOrderedSnapshot() throws Exception {
    testSnapshot(Ordering.NATURAL);
  }

  @Test
  public void testInsertionOrderedSnapshot() throws Exception {
    testSnapshot(Ordering.INSERTION);
  }

  private void testSnapshot(Ordering ordering) throws Exception {
    DocumentTreeService service = new DocumentTreeService(ordering);
    service.update(new DefaultCommit<>(
        2,
        UPDATE,
        new Update(
            DocumentPath.from("root|foo"),
            Optional.of("Hello world!".getBytes()),
            Match.any(),
            Match.ifNull()),
        mock(Session.class),
        System.currentTimeMillis()));

    Buffer buffer = HeapBuffer.allocate();
    service.backup(buffer);

    service = new DocumentTreeService(ordering);
    service.restore(buffer.flip());

    Versioned<byte[]> value = service.get(new DefaultCommit<>(
        2,
        GET,
        new Get(DocumentPath.from("root|foo")),
        mock(Session.class),
        System.currentTimeMillis()));
    assertNotNull(value);
    assertArrayEquals("Hello world!".getBytes(), value.value());
  }
}
