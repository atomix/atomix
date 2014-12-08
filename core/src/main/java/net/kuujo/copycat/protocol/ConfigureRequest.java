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

import java.util.Set;

/**
 * Protocol configure request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ConfigureRequest extends AbstractRequest {
  public static final int TYPE = -7;

  /**
   * Returns a new configure request builder.
   *
   * @return A new configure request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private Set<String> members;

  /**
   * Returns the cluster members.
   *
   * @return The cluster members.
   */
  public Set<String> members() {
    return members;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s, members=%s]", getClass().getSimpleName(), id, members);
  }

  /**
   * Configure request builder.
   */
  public static class Builder extends AbstractRequest.Builder<Builder, ConfigureRequest> {
    private Builder() {
      super(new ConfigureRequest());
    }

    /**
     * Sets the request member configuration.
     *
     * @param members The request member configurations.
     * @return The configure request builder.
     */
    public Builder withMembers(Set<String> members) {
      request.members = members;
      return this;
    }

  }

}
