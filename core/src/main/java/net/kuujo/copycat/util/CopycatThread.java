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

import java.lang.ref.WeakReference;

/**
 * Copycat thread.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopycatThread extends Thread {
  private WeakReference<ExecutionContext> context;

  public CopycatThread(Runnable target, String name) {
    super(target, name);
  }

  /**
   * Sets the thread context.
   */
  public void setContext(ExecutionContext context) {
    this.context = new WeakReference<>(context);
  }

  /**
   * Returns the thread context.
   */
  public ExecutionContext getContext() {
    return context.get();
  }

}
