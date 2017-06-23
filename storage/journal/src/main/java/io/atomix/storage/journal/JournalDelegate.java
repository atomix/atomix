/*
 * Copyright 2017-present Open Networking Laboratory
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
package io.atomix.storage.journal;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Journal delegate.
 */
public class JournalDelegate<E> implements Journal<E> {
  private final Journal<E> delegate;

  public JournalDelegate(Journal<E> delegate) {
    this.delegate = delegate;
  }

  @Override
  public JournalWriter<E> getWriter() {
    return delegate.getWriter();
  }

  @Override
  public JournalReader<E> openReader(long index) {
    return delegate.openReader(index);
  }

  @Override
  public boolean isOpen() {
    return delegate.isOpen();
  }

  @Override
  public void compact(long index) {
    delegate.compact(index);
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("delegate", delegate)
        .toString();
  }
}
