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

import net.kuujo.copycat.log.Entry;

/**
 * Protocol commit request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CommitRequest extends AbstractRequest {

  /**
   * Returns a new commit request builder.
   *
   * @return A new commit request builder.
   */
  static Builder builder() {
    return new Builder();
  }

  private Object entry;

  /**
   * Returns the entry to be committed.
   *
   * @return The entry to be committed.
   */
  @SuppressWarnings("unchecked")
  public <T> T entry() {
    return (T) entry;
  }

  /**
   * Commit request builder.
   */
  public static class Builder extends AbstractRequest.Builder<Builder, CommitRequest> {
    private Builder() {
      super(new CommitRequest());
    }

    /**
     * Sets the commit request entry.
     *
     * @param entry The commit request entry.
     * @return The commit request builder.
     */
    public Builder withEntry(Entry entry) {
      request.entry = entry;
      return this;
    }

  }

}
