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
 * Protocol configure response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ConfigureResponse extends AbstractResponse {
  public static final int TYPE = -8;

  /**
   * Returns a new configure response builder.
   *
   * @return A new configure response builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s]", getClass().getSimpleName(), id);
  }

  /**
   * Configure response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, ConfigureResponse> {
    private Builder() {
      super(new ConfigureResponse());
    }
  }

}
