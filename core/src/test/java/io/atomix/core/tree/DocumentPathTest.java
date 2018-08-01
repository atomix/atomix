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
