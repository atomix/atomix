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
 * Abstract response implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class AbstractResponse implements Response {
  protected Object id;
  protected String member;
  protected Status status = Status.OK;
  protected Throwable error;

  @Override
  public Object id() {
    return id;
  }

  @Override
  public String uri() {
    return member;
  }

  @Override
  public Status status() {
    return status;
  }

  @Override
  public Throwable error() {
    return error;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, member, status);
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s, uri=%s, status=%s]", getClass().getCanonicalName(), id, member, status);
  }

  /**
   * Abstract response builder.
   *
   * @param <T> The builder type.
   * @param <U> The response type.
   */
  protected static abstract class Builder<T extends Builder<T, U>, U extends AbstractResponse> implements Response.Builder<T, U> {
    protected final U response;

    protected Builder(U response) {
      this.response = response;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T withId(Object id) {
      Assert.isNotNull(id, "id");
      response.id = id;
      return (T) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T withUri(String member) {
      Assert.isNotNull(member, "uri");
      response.member = member;
      return (T) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T withStatus(Status status) {
      Assert.isNotNull(status, "status");
      response.status = status;
      return (T) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T withError(Throwable error) {
      Assert.isNotNull(error, "error");
      response.error = error;
      return (T) this;
    }

    @Override
    public U build() {
      Assert.isNotNull(response.id, "id");
      Assert.isNotNull(response.member, "uri");
      Assert.isNotNull(response.status, "status");
      return response;
    }
  }

}
