/*
 * Copyright 2015 the original author or authors.
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

import net.kuujo.copycat.EventListener;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Local protocol connection implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalProtocolConnection implements ProtocolConnection {
  private ProtocolHandler handler;
  private EventListener<Void> closeListener;

  @Override
  public void handler(ProtocolHandler handler) {
    this.handler = handler;
  }

  @Override
  public CompletableFuture<ByteBuffer> write(ByteBuffer request) {
    return handler.apply(request.duplicate()).thenApply(ByteBuffer::duplicate);
  }

  @Override
  public ProtocolConnection closeListener(EventListener<Void> listener) {
    this.closeListener = listener;
    return this;
  }

  @Override
  public ProtocolConnection exceptionListener(EventListener<Throwable> listener) {
    return this;
  }

  @Override
  public CompletableFuture<Void> close() {
    if (closeListener != null) {
      closeListener.accept(null);
    }
    return CompletableFuture.completedFuture(null);
  }

}
