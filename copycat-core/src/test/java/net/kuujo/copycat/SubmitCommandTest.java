/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat;

import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

/**
 * Submit command test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SubmitCommandTest extends CopycatTest {

  @Test
  public void testCopyCat() throws Exception {
    new RunnableTest() {
      @Override
      public void run() throws Exception {
        Set<CopycatContext> contexts = startCluster(3);
        final CopycatContext context = contexts.iterator().next();
        context.on().leaderElect().run((event) -> {
          context.submitCommand("set", "foo", "bar").thenRun(() -> {
            context.submitCommand("set", "bar", "baz").thenRun(() -> {
              context.submitCommand("get", "foo").whenComplete((result, error) -> {
                Assert.assertNull(error);
                Assert.assertEquals("bar", result);
              });
            });
          });
        });
      }
    }.start();
  }

}
