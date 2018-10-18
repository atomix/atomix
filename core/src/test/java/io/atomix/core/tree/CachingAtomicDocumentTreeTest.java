/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.tree;

/**
 * Caching document tree test.
 */
public class CachingAtomicDocumentTreeTest extends AtomicDocumentTreeTest {
  @Override
  protected AtomicDocumentTree<String> newTree(String name) throws Exception {
    return atomix().<String>atomicDocumentTreeBuilder(name)
        .withProtocol(protocol())
        .withCacheEnabled()
        .build();
  }
}
