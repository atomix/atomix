/*
 * Copyright 2014 the original author or authors.
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
 * Quorum helper.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Quorum {
  private int succeeded;
  private int failed;
  private int quorum;
  private AsyncCallback<Boolean> callback;
  private boolean complete;

  public Quorum(int quorum) {
    this.quorum = quorum;
  }

  /**
   * Sets the quorum callback.
   *
   * @param callback The quorum callback.
   * @return The quorum instance.
   */
  public Quorum setCallback(AsyncCallback<Boolean> callback) {
    this.callback = callback;
    return this;
  }

  /**
   * Counts the current node in the quorum.
   */
  public Quorum countSelf() {
    quorum++;
    succeeded++;
    return this;
  }

  private void checkComplete() {
    if (!complete && callback != null) {
      if (succeeded >= quorum) {
        complete = true;
        callback.complete(true);
      } else if (failed >= quorum) {
        complete = true;
        callback.complete(false);
      }
    }
  }

  /**
   * Indicates that a call in the quorum succeeded.
   */
  public Quorum succeed() {
    succeeded++;
    checkComplete();
    return this;
  }

  /**
   * Indicates that a call in the quorum failed.
   */
  public Quorum fail() {
    failed++;
    checkComplete();
    return this;
  }

  /**
   * Cancels the quorum. Once this method has been called, the quorum
   * will be marked complete and the handler will never be called.
   */
  public void cancel() {
    callback = null;
    complete = true;
  }

}
