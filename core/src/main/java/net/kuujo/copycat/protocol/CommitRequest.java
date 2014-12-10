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

/**
 * Protocol commit request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CommitRequest extends AbstractRequest {
  public static final int TYPE = -13;

  /**
   * Returns a new commit request builder.
   *
   * @return A new commit request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private String action;
  private Object entry;

  /**
   * Returns commit action.
   *
   * @return The commit action.
   */
  public String action() {
    return action;
  }

  /**
   * Returns the commit entry.
   *
   * @return The commit entry.
   */
  @SuppressWarnings("unchecked")
  public <T> T entry() {
    return (T) entry;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s, action=%s]", getClass().getSimpleName(), id, action);
  }

  /**
   * Commit request builder.
   */
  public static class Builder extends AbstractRequest.Builder<Builder, CommitRequest> {
    private Builder() {
      super(new CommitRequest());
    }

    /**
     * Sets the request action.
     *
     * @param action The request action.
     * @return The request builder.
     */
    public Builder withAction(String action) {
      request.action = action;
      return this;
    }

    /**
     * Sets the request entry.
     *
     * @param entry The request entry.
     * @return The request builder.
     */
    public Builder withEntry(Object entry) {
      request.entry = entry;
      return this;
    }

  }

}
