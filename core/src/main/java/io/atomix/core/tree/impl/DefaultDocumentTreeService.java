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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import io.atomix.core.tree.AtomicDocumentTree;
import io.atomix.core.tree.AtomicDocumentTreeType;
import io.atomix.core.tree.DocumentPath;
import io.atomix.core.tree.DocumentTreeEvent;
import io.atomix.core.tree.IllegalDocumentModificationException;
import io.atomix.core.tree.NoSuchDocumentPathException;
import io.atomix.primitive.Ordering;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.Versioned;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * State Machine for {@link AtomicDocumentTreeProxy} resource.
 */
public class DefaultDocumentTreeService extends AbstractPrimitiveService<DocumentTreeClient> implements DocumentTreeService {
  private final Serializer serializer = Serializer.using(Namespace.builder()
      .register(AtomicDocumentTreeType.instance().namespace())
      .register(Versioned.class)
      .register(DocumentPath.class)
      .register(new LinkedHashMap().keySet().getClass())
      .register(TreeMap.class)
      .register(Ordering.class)
      .register(SessionListenCommits.class)
      .register(SessionId.class)
      .register(new com.esotericsoftware.kryo.Serializer<DefaultAtomicDocumentTree>() {
        @Override
        public void write(Kryo kryo, Output output, DefaultAtomicDocumentTree object) {
          kryo.writeObject(output, object.root);
        }

        @Override
        @SuppressWarnings("unchecked")
        public DefaultAtomicDocumentTree read(Kryo kryo, Input input, Class<DefaultAtomicDocumentTree> type) {
          return new DefaultAtomicDocumentTree(versionCounter::incrementAndGet,
              kryo.readObject(input, DefaultDocumentTreeNode.class));
        }
      }, DefaultAtomicDocumentTree.class)
      .register(DefaultDocumentTreeNode.class)
      .build());

  private Map<SessionId, SessionListenCommits> listeners = new HashMap<>();
  private AtomicLong versionCounter = new AtomicLong(0);
  private AtomicDocumentTree<byte[]> docTree;
  private Set<DocumentPath> preparedKeys = Sets.newHashSet();

  public DefaultDocumentTreeService() {
    super(AtomicDocumentTreeType.instance(), DocumentTreeClient.class);
    this.docTree = new DefaultAtomicDocumentTree<>(versionCounter::incrementAndGet, Ordering.NATURAL);
  }

  @Override
  public Serializer serializer() {
    return serializer;
  }

  @Override
  public void backup(BackupOutput writer) {
    writer.writeLong(versionCounter.get());
    writer.writeObject(listeners);
    writer.writeObject(docTree);
    writer.writeObject(preparedKeys);
  }

  @Override
  public void restore(BackupInput reader) {
    versionCounter = new AtomicLong(reader.readLong());
    listeners = reader.readObject();
    docTree = reader.readObject();
    preparedKeys = reader.readObject();
  }

  /**
   * Returns a boolean indicating whether the given path is currently locked by a transaction.
   *
   * @param path the path to check
   * @return whether the given path is locked by a running transaction
   */
  private boolean isLocked(DocumentPath path) {
    return preparedKeys.contains(path);
  }

  @Override
  public void listen(DocumentPath path) {
    listeners.computeIfAbsent(getCurrentSession().sessionId(), k -> new SessionListenCommits()).add(path);
  }

  @Override
  public void unlisten(DocumentPath path) {
    SessionListenCommits listenCommits = listeners.get(getCurrentSession().sessionId());
    if (listenCommits != null) {
      listenCommits.remove(path);
    }
  }

  @Override
  public Versioned<byte[]> get(DocumentPath path) {
    try {
      Versioned<byte[]> value = docTree.get(path);
      return value == null ? null : value.map(node -> node);
    } catch (IllegalStateException e) {
      return null;
    }
  }

  @Override
  public DocumentTreeResult<Map<String, Versioned<byte[]>>> getChildren(DocumentPath path) {
    try {
      return DocumentTreeResult.ok(docTree.getChildren(path));
    } catch (NoSuchDocumentPathException e) {
      return DocumentTreeResult.invalidPath();
    }
  }

  @Override
  public DocumentTreeResult<Versioned<byte[]>> set(DocumentPath path, byte[] value) {
    try {
      Versioned<byte[]> oldValue = docTree.get(path);
      if (oldValue == null) {
        docTree.set(path, value);
        notifyListeners(new DocumentTreeEvent<>(DocumentTreeEvent.Type.CREATED, path, Optional.of(docTree.get(path)), Optional.empty()));
      } else if (!Arrays.equals(oldValue.value(), value)) {
        docTree.set(path, value);
        notifyListeners(new DocumentTreeEvent<>(DocumentTreeEvent.Type.UPDATED, path, Optional.of(docTree.get(path)), Optional.of(oldValue)));
      }
      return DocumentTreeResult.ok(oldValue);
    } catch (NoSuchDocumentPathException e) {
      return DocumentTreeResult.invalidPath();
    } catch (IllegalDocumentModificationException e) {
      return DocumentTreeResult.illegalModification();
    }
  }

  @Override
  public DocumentTreeResult<Void> create(DocumentPath path, byte[] value) {
    try {
      if (docTree.create(path, value)) {
        notifyListeners(new DocumentTreeEvent<>(DocumentTreeEvent.Type.CREATED, path, Optional.of(docTree.get(path)), Optional.empty()));
        return DocumentTreeResult.ok(null);
      }
      return DocumentTreeResult.NOOP;
    } catch (IllegalDocumentModificationException e) {
      return DocumentTreeResult.illegalModification();
    } catch (NoSuchDocumentPathException e) {
      return DocumentTreeResult.invalidPath();
    }
  }

  @Override
  public DocumentTreeResult<Void> createRecursive(DocumentPath path, byte[] value) {
    try {
      if (docTree.create(path, value)) {
        notifyListeners(new DocumentTreeEvent<>(DocumentTreeEvent.Type.CREATED, path, Optional.of(docTree.get(path)), Optional.empty()));
        return DocumentTreeResult.ok(null);
      }
      return DocumentTreeResult.NOOP;
    } catch (IllegalDocumentModificationException | NoSuchDocumentPathException e) {
      createRecursive(path.parent(), null);
      return create(path, value);
    }
  }

  @Override
  public DocumentTreeResult<Void> replace(DocumentPath path, byte[] newValue, long version) {
    try {
      Versioned<byte[]> oldValue = docTree.get(path);
      if (docTree.replace(path, newValue, version)) {
        notifyListeners(new DocumentTreeEvent<>(DocumentTreeEvent.Type.UPDATED, path, Optional.of(docTree.get(path)), Optional.of(oldValue)));
        return DocumentTreeResult.ok(null);
      }
      return DocumentTreeResult.NOOP;
    } catch (NoSuchDocumentPathException e) {
      return DocumentTreeResult.invalidPath();
    }
  }

  @Override
  public DocumentTreeResult<Void> replace(DocumentPath path, byte[] newValue, byte[] currentValue) {
    try {
      Versioned<byte[]> oldValue = docTree.get(path);
      if (oldValue == null || oldValue.value() == null && currentValue == null) {
        return DocumentTreeResult.NOOP;
      } else if (oldValue.value() != null && currentValue != null && Arrays.equals(oldValue.value(), currentValue)) {
        docTree.set(path, newValue);
        notifyListeners(new DocumentTreeEvent<>(DocumentTreeEvent.Type.UPDATED, path, Optional.of(docTree.get(path)), Optional.of(oldValue)));
        return DocumentTreeResult.ok(null);
      }
      return DocumentTreeResult.NOOP;
    } catch (NoSuchDocumentPathException e) {
      return DocumentTreeResult.invalidPath();
    }
  }

  @Override
  public DocumentTreeResult<Versioned<byte[]>> removeNode(DocumentPath path) {
    try {
      Versioned<byte[]> result = docTree.remove(path);
      notifyListeners(new DocumentTreeEvent<>(DocumentTreeEvent.Type.DELETED, path, Optional.empty(), Optional.of(result)));
      return DocumentTreeResult.ok(result);
    } catch (IllegalDocumentModificationException e) {
      return DocumentTreeResult.illegalModification();
    } catch (NoSuchDocumentPathException e) {
      return DocumentTreeResult.invalidPath();
    }
  }

  @Override
  public void clear() {
    Queue<DocumentPath> toClearQueue = Queues.newArrayDeque();
    Map<String, Versioned<byte[]>> topLevelChildren = docTree.getChildren(DocumentPath.ROOT);
    toClearQueue.addAll(topLevelChildren.keySet()
        .stream()
        .map(name -> new DocumentPath(name, DocumentPath.ROOT))
        .collect(Collectors.toList()));
    while (!toClearQueue.isEmpty()) {
      DocumentPath path = toClearQueue.remove();
      Map<String, Versioned<byte[]>> children = docTree.getChildren(path);
      if (children.size() == 0) {
        docTree.remove(path);
      } else {
        children.keySet().forEach(name -> toClearQueue.add(new DocumentPath(name, path)));
        toClearQueue.add(path);
      }
    }
  }

  private void notifyListeners(DocumentTreeEvent<byte[]> event) {
    listeners.entrySet()
        .stream()
        .filter(e -> event.path().isDescendentOf(e.getValue().leastCommonAncestorPath()))
        .forEach(e -> getSession(e.getKey()).accept(client -> client.change(event)));
  }

  @Override
  public void onExpire(Session session) {
    closeListener(session.sessionId());
  }

  @Override
  public void onClose(Session session) {
    closeListener(session.sessionId());
  }

  private void closeListener(SessionId sessionId) {
    listeners.remove(sessionId);
  }

  private class SessionListenCommits {
    private final Set<DocumentPath> paths = Sets.newHashSet();
    private DocumentPath leastCommonAncestorPath;

    public void add(DocumentPath path) {
      paths.add(path);
      recomputeLeastCommonAncestor();
    }

    public void remove(DocumentPath path) {
      paths.remove(path);
      recomputeLeastCommonAncestor();
    }

    public DocumentPath leastCommonAncestorPath() {
      return leastCommonAncestorPath;
    }

    private void recomputeLeastCommonAncestor() {
      this.leastCommonAncestorPath = DocumentPath.leastCommonAncestor(paths);
    }
  }
}
