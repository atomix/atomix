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
package net.kuujo.copycat.protocol;

import java.util.Objects;

/**
 * Protocol sync request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SyncRequest extends AbstractRequest {
  public static final int TYPE = -13;

  /**
   * Returns a new sync request builder.
   *
   * @return A new sync request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof SyncRequest) {
      SyncRequest request = (SyncRequest) object;
      return request.id.equals(id)
        && request.member.equals(member);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s]", getClass().getSimpleName(), id);
  }

  /**
   * Commit request builder.
   */
  public static class Builder extends AbstractRequest.Builder<Builder, SyncRequest> {
    private Builder() {
      super(new SyncRequest());
    }

    @Override
    public int hashCode() {
      return Objects.hash(request);
    }

    @Override
    public boolean equals(Object object) {
      return object instanceof Builder && ((Builder) object).request.equals(request);
    }

    @Override
    public String toString() {
      return String.format("%s[request=%s]", getClass().getCanonicalName(), request);
    }

  }

}
