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
package io.atomix.utils;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Version test.
 */
public class VersionTest {
  @Test
  public void testVersionComparison() {
    assertTrue(Version.from("1.0.0").compareTo(Version.from("2.0.0")) < 0);
    assertTrue(Version.from("2.0.0").compareTo(Version.from("1.0.0")) > 0);
    assertTrue(Version.from("1.0.0").compareTo(Version.from("0.1.0")) > 0);
    assertTrue(Version.from("0.1.0").compareTo(Version.from("1.0.0")) < 0);
    assertTrue(Version.from("0.1.0").compareTo(Version.from("0.1.1")) < 0);
    assertTrue(Version.from("1.0.0").compareTo(Version.from("0.0.1")) > 0);
    assertTrue(Version.from("1.1.1").compareTo(Version.from("1.0.3")) > 0);
    assertTrue(Version.from("1.0.0").compareTo(Version.from("1.0.0-beta1")) > 0);
    assertTrue(Version.from("1.0.0-rc2").compareTo(Version.from("1.0.0-rc1")) > 0);
    assertTrue(Version.from("1.0.0-rc1").compareTo(Version.from("1.0.0-beta1")) > 0);
    assertTrue(Version.from("2.0.0-beta1").compareTo(Version.from("1.0.0")) > 0);
  }
}
