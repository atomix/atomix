/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.iterator.impl;

import io.atomix.core.iterator.AsyncIterator;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Transcoding iterator.
 */
public class TranscodingIterator<T1, T2> implements AsyncIterator<T1> {
  private final AsyncIterator<T2> backingIterator;
  private final Function<T2, T1> elementDecoder;

  public TranscodingIterator(AsyncIterator<T2> backingIterator, Function<T2, T1> elementDecoder) {
    this.backingIterator = backingIterator;
    this.elementDecoder = elementDecoder;
  }

  @Override
  public CompletableFuture<Boolean> hasNext() {
    return backingIterator.hasNext();
  }

  @Override
  public CompletableFuture<T1> next() {
    return backingIterator.next().thenApply(elementDecoder);
  }

  @Override
  public CompletableFuture<Void> close() {
    return backingIterator.close();
  }
}
