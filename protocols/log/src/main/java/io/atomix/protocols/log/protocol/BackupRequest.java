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
package io.atomix.protocols.log.protocol;

import io.atomix.cluster.MemberId;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Backup request.
 */
public class BackupRequest extends LogRequest {

  public static BackupRequest request(MemberId leader, long term, long index, List<BackupOperation> batch) {
    return new BackupRequest(leader, term, index, batch);
  }

  private final MemberId leader;
  private final long term;
  private final long commitIndex;
  private final List<BackupOperation> batch;

  public BackupRequest(MemberId leader, long term, long commitIndex, List<BackupOperation> batch) {
    this.leader = leader;
    this.term = term;
    this.commitIndex = commitIndex;
    this.batch = batch;
  }

  public MemberId leader() {
    return leader;
  }

  public long term() {
    return term;
  }

  public long index() {
    return commitIndex;
  }

  public List<BackupOperation> batch() {
    return batch;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("leader", leader())
        .add("term", term())
        .add("index", index())
        .add("batch", batch())
        .toString();
  }
}
