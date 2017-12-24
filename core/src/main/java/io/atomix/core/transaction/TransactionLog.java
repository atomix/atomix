/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.transaction;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * Collection of transaction updates to be applied atomically.
 *
 * @param <T> log record type
 */
public class TransactionLog<T> {
  private final TransactionId transactionId;
  private final long version;
  private final List<T> records;

  public TransactionLog(TransactionId transactionId, long version, List<T> records) {
    this.transactionId = transactionId;
    this.version = version;
    this.records = ImmutableList.copyOf(records);
  }

  /**
   * Returns the transaction identifier.
   *
   * @return transaction id
   */
  public TransactionId transactionId() {
    return transactionId;
  }

  /**
   * Returns the transaction lock version.
   *
   * @return the transaction lock version
   */
  public long version() {
    return version;
  }

  /**
   * Returns the list of transaction log records.
   *
   * @return a list of transaction log records
   */
  public List<T> records() {
    return records;
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof TransactionLog) {
      TransactionLog that = (TransactionLog) object;
      return this.transactionId.equals(that.transactionId)
          && this.records.equals(that.records);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(transactionId, records);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("transactionId", transactionId)
        .add("version", version)
        .add("records", records)
        .toString();
  }

  /**
   * Maps this instance to another {@code MapTransaction} with different key and value types.
   *
   * @param mapper function for mapping record types
   * @param <U>    record type of returned instance
   * @return newly typed instance
   */
  public <U> TransactionLog<U> map(Function<T, U> mapper) {
    return new TransactionLog<>(transactionId, version, Lists.transform(records, mapper::apply));
  }
}