// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.tree;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Document path test.
 */
public class DocumentPathTest {
  @Test
  public void testValidDocumentPath() {
    assertEquals("/", DocumentPath.from("/").toString());
    assertEquals(1, DocumentPath.from("/").pathElements().size());
    assertEquals("/foo", DocumentPath.from("/foo").toString());
    assertEquals(2, DocumentPath.from("/foo").pathElements().size());
    assertEquals("/foo/bar", DocumentPath.from("/foo/bar").toString());
    assertEquals(3, DocumentPath.from("/foo/bar").pathElements().size());
    assertEquals("foo", DocumentPath.from("/foo").childPath().toString());
    assertEquals("/", DocumentPath.from("/foo").parent().toString());
    assertEquals("bar", DocumentPath.from("/foo/bar").childPath().toString());
    assertEquals("/foo", DocumentPath.from("/foo/bar").parent().toString());
    assertEquals("foo", DocumentPath.from("foo").toString());
    assertEquals("foo/bar", DocumentPath.from("foo/bar").toString());
    assertEquals(1, DocumentPath.from("foo").pathElements().size());
    assertEquals(2, DocumentPath.from("foo/bar").pathElements().size());
  }

  @Test
  public void testStaticFactories() throws Exception {
    assertEquals("/foo", DocumentPath.from("/foo").toString());
    assertEquals("/foo", DocumentPath.from(new String[]{"foo"}).toString());
    assertEquals("/foo/bar", DocumentPath.from("foo", "bar").toString());
    assertEquals("/foo", DocumentPath.from(Arrays.asList("foo")).toString());
    assertEquals("/foo/bar", DocumentPath.from(Arrays.asList("foo", "bar")).toString());
    assertEquals("/foo/bar", DocumentPath.from(Arrays.asList("foo"), "bar").toString());
    assertEquals("/foo/bar/baz", DocumentPath.from(Arrays.asList("foo", "bar"), "baz").toString());
  }
}
