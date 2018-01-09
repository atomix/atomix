/*
 * Copyright 2016-present Open Networking Foundation
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

package io.atomix.core.tree.impl;

import com.google.common.base.MoreObjects;
import io.atomix.core.map.impl.CommitResult;
import io.atomix.core.map.impl.PrepareResult;
import io.atomix.core.map.impl.RollbackResult;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.core.tree.DocumentPath;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.OperationType;
import io.atomix.utils.Match;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.time.Versioned;

import java.util.LinkedHashMap;
import java.util.Optional;

/**
 * {@link io.atomix.core.tree.DocumentTree} operations.
 * <p>
 * WARNING: Do not refactor enum values. Only add to them.
 * Changing values risk breaking the ability to backup/restore/upgrade clusters.
 */
public enum DocumentTreeOperations implements OperationId {
  ADD_LISTENER(OperationType.COMMAND),
  REMOVE_LISTENER(OperationType.COMMAND),
  GET(OperationType.QUERY),
  GET_CHILDREN(OperationType.QUERY),
  UPDATE(OperationType.COMMAND),
  CLEAR(OperationType.COMMAND),
  BEGIN(OperationType.COMMAND),
  PREPARE(OperationType.COMMAND),
  PREPARE_AND_COMMIT(OperationType.COMMAND),
  COMMIT(OperationType.COMMAND),
  ROLLBACK(OperationType.COMMAND);

  private final OperationType type;

  DocumentTreeOperations(OperationType type) {
    this.type = type;
  }

  @Override
  public String id() {
    return name();
  }

  @Override
  public OperationType type() {
    return type;
  }

  public static final KryoNamespace NAMESPACE = KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
      .register(LinkedHashMap.class)
      .register(Listen.class)
      .register(Unlisten.class)
      .register(Get.class)
      .register(GetChildren.class)
      .register(Update.class)
      .register(TransactionId.class)
      .register(TransactionLog.class)
      .register(PrepareResult.class)
      .register(CommitResult.class)
      .register(RollbackResult.class)
      .register(NodeUpdate.class)
      .register(NodeUpdate.Type.class)
      .register(DocumentPath.class)
      .register(Match.class)
      .register(Versioned.class)
      .register(DocumentTreeResult.class)
      .register(DocumentTreeResult.Status.class)
      .build(DocumentTreeOperations.class.getSimpleName());

  /**
   * Base class for document tree operations.
   */
  public abstract static class DocumentTreeOperation {
  }

  /**
   * Base class for document tree operations that serialize a {@link DocumentPath}.
   */
  @SuppressWarnings("serial")
  public abstract static class PathOperation extends DocumentTreeOperation {
    private DocumentPath path;

    PathOperation(DocumentPath path) {
      this.path = path;
    }

    public DocumentPath path() {
      return path;
    }
  }

  /**
   * DocumentTree#get query.
   */
  @SuppressWarnings("serial")
  public static class Get extends PathOperation {
    public Get() {
      super(null);
    }

    public Get(DocumentPath path) {
      super(path);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("path", path())
          .toString();
    }
  }

  /**
   * DocumentTree#getChildren query.
   */
  @SuppressWarnings("serial")
  public static class GetChildren extends PathOperation {
    public GetChildren() {
      super(null);
    }

    public GetChildren(DocumentPath path) {
      super(path);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("path", path())
          .toString();
    }
  }

  /**
   * DocumentTree update command.
   */
  @SuppressWarnings("serial")
  public static class Update extends PathOperation {
    private Optional<byte[]> value;
    private Match<byte[]> valueMatch;
    private Match<Long> versionMatch;

    public Update() {
      super(null);
      this.value = null;
      this.valueMatch = null;
      this.versionMatch = null;
    }

    public Update(DocumentPath path, Optional<byte[]> value, Match<byte[]> valueMatch, Match<Long> versionMatch) {
      super(path);
      this.value = value;
      this.valueMatch = valueMatch;
      this.versionMatch = versionMatch;
    }

    public Optional<byte[]> value() {
      return value;
    }

    public Match<byte[]> valueMatch() {
      return valueMatch;
    }

    public Match<Long> versionMatch() {
      return versionMatch;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("path", path())
          .add("value", value)
          .add("valueMatch", valueMatch)
          .add("versionMatch", versionMatch)
          .toString();
    }
  }

  /**
   * Change listen.
   */
  @SuppressWarnings("serial")
  public static class Listen extends PathOperation {
    public Listen() {
      this(DocumentPath.from("root"));
    }

    public Listen(DocumentPath path) {
      super(path);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("path", path())
          .toString();
    }
  }

  /**
   * Change unlisten.
   */
  @SuppressWarnings("serial")
  public static class Unlisten extends PathOperation {
    public Unlisten() {
      this(DocumentPath.from("root"));
    }

    public Unlisten(DocumentPath path) {
      super(path);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("path", path())
          .toString();
    }
  }
}
