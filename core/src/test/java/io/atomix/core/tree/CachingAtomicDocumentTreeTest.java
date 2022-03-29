// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
