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
package net.kuujo.copycat.protocol;

import net.kuujo.copycat.Task;

/**
 * Protocol submit request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SubmitRequest extends AbstractRequest {
  public static final int TYPE = -13;

  /**
   * Returns a new submit request builder.
   *
   * @return A new submit request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  @SuppressWarnings("rawtypes")
  private Task task;

  /**
   * Returns the submission task.
   *
   * @return The task to execute.
   */
  @SuppressWarnings("rawtypes")
  public Task task() {
    return task;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s", getClass().getSimpleName(), id);
  }

  /**
   * Submit request builder.
   */
  public static class Builder extends AbstractRequest.Builder<Builder, SubmitRequest> {
    private Builder() {
      super(new SubmitRequest());
    }

    /**
     * Sets the request task.
     *
     * @param task The request task.
     * @return The request builder.
     */
    public Builder withTask(Task task) {
      request.task = task;
      return this;
    }

  }

}
