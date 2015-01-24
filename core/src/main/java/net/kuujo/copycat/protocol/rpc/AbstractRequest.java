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
package net.kuujo.copycat.protocol.rpc;

import net.kuujo.copycat.util.internal.Assert;

import java.util.Objects;

/**
 * Abstract request implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class AbstractRequest implements Request {
  protected Object id;
  protected String member;

  @Override
  public Object id() {
    return id;
  }

  @Override
  public String uri() {
    return member;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, member);
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s, uri=%s]", getClass().getCanonicalName(), id, member);
  }

  /**
   * Abstract request builder.
   *
   * @param <T> The builder type.
   * @param <U> The request type.
   */
  protected static abstract class Builder<T extends Builder<T, U>, U extends AbstractRequest> implements Request.Builder<T, U> {
    protected final U request;

    protected Builder(U request) {
      this.request = request;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T withId(Object id) {
      Assert.isNotNull(id, "id");
      request.id = id;
      return (T) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T withUri(String uri) {
      Assert.isNotNull(uri, "uri");
      request.member = uri;
      return (T) this;
    }

    @Override
    public U build() {
      Assert.isNotNull(request.id, "id");
      Assert.isNotNull(request.member, "uri");
      return request;
    }
  }

}
