/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.protocols.backup.protocol;

import io.atomix.cluster.MemberId;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Backup request.
 */
public class BackupRequest extends PrimitiveRequest {

  public static BackupRequest request(PrimitiveDescriptor primitive, MemberId primary, long term, long index, List<BackupOperation> operations) {
    return new BackupRequest(primitive, primary, term, index, operations);
  }

  private final MemberId primary;
  private final long term;
  private final long index;
  private final List<BackupOperation> operations;

  public BackupRequest(PrimitiveDescriptor primitive, MemberId primary, long term, long index, List<BackupOperation> operations) {
    super(primitive);
    this.primary = primary;
    this.term = term;
    this.index = index;
    this.operations = operations;
  }

  public MemberId primary() {
    return primary;
  }

  public long term() {
    return term;
  }

  public long index() {
    return index;
  }

  public List<BackupOperation> operations() {
    return operations;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("primary", primary())
        .add("term", term())
        .add("index", index())
        .add("primitive", primitive())
        .add("operations", operations())
        .toString();
  }
}
