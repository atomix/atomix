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
 * Protocol submit request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CommitRequest extends AbstractRequest {
  public static final int TYPE = -11;

  /**
   * Returns a new submit request builder.
   *
   * @return A new submit request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private Object entry;
  private boolean consistent = true;
  private boolean persistent = true;

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
   * Returns a boolean indicating whether the submit is consistent.
   *
   * @return Indicates whether the submit is consistent.
   */
  public boolean consistent() {
    return consistent;
  }

  /**
   * Returns a boolean indicating whether the submit is persistent.
   *
   * @return Indicates whether the submit is persistent.
   */
  public boolean persistent() {
    return persistent;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s, entry=%s]", getClass().getSimpleName(), id, entry);
  }

  /**
   * Commit request builder.
   */
  public static class Builder extends AbstractRequest.Builder<Builder, CommitRequest> {
    private Builder() {
      super(new CommitRequest());
    }

    /**
     * Sets the submit request entry.
     *
     * @param entry The submit request entry.
     * @return The submit request builder.
     */
    public Builder withEntry(Object entry) {
      request.entry = entry;
      return this;
    }

    /**
     * Sets whether the submit is consistent.
     *
     * @param consistent Whether the submit is persistent.
     * @return The submit request builder.
     */
    public Builder withConsistent(boolean consistent) {
      request.consistent = consistent;
      return this;
    }

    /**
     * Sets whether the submit is persistent.
     *
     * @param persistent Whether the submit is persistent.
     * @return The submit request builder.
     */
    public Builder withPersistent(boolean persistent) {
      request.persistent = persistent;
      return this;
    }

  }

}
