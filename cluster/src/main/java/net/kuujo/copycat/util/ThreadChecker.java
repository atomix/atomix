/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.util;

/**
 * Context thread checker.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ThreadChecker {
  private final ExecutionContext context;

  public ThreadChecker(ExecutionContext context) {
    this.context = context;
  }

  /**
   * Checks that the current thread is the correct context thread.
   */
  public void checkThread() {
    Thread thread = Thread.currentThread();
    if (!(thread instanceof net.kuujo.copycat.util.CopycatThread && ((net.kuujo.copycat.util.CopycatThread) thread).getContext() == context)) {
      throw new IllegalStateException("not running on the correct thread");
    }
  }

}
