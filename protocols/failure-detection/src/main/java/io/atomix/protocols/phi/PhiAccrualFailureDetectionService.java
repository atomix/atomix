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
package io.atomix.protocols.phi;

import com.google.common.collect.Maps;
import io.atomix.event.AbstractListenerManager;
import io.atomix.protocols.phi.protocol.FailureDetectionProtocol;
import io.atomix.protocols.phi.protocol.HeartbeatMessage;
import io.atomix.utils.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Phi-accrual failure detection service.
 */
public class PhiAccrualFailureDetectionService<T extends Identifier>
    extends AbstractListenerManager<FailureDetectionEvent<T>, FailureDetectionEventListener<T>>
    implements FailureDetectionService<T> {

  /**
   * Returns a new phi accrual failure detection service builder.
   *
   * @param <T> the node type
   * @return a new phi accrual failure detection service builder
   */
  public static <T extends Identifier> Builder<T> builder() {
    return new Builder<>();
  }

  private Logger log = LoggerFactory.getLogger(getClass());
  private final T localNode;
  private final FailureDetectionProtocol<T> protocol;
  private final Supplier<Collection<T>> peerProvider;
  private final ScheduledFuture<?> heartbeatFuture;
  private final int phiFailureThreshold;
  private final int minSamples;
  private final double phiFactor;
  private final Map<T, PhiAccrualFailureDetector> nodes = Maps.newConcurrentMap();

  private final Map<T, FailureDetectionEvent.State> nodeStates = Maps.newConcurrentMap();

  public PhiAccrualFailureDetectionService(
      FailureDetectionProtocol<T> protocol,
      T localNode,
      Supplier<Collection<T>> peerProvider,
      ScheduledExecutorService heartbeatExecutor,
      Duration heartbeatInterval,
      int phiFailureThreshold,
      int minSamples,
      double phiFactor) {
    checkArgument(phiFailureThreshold > 0, "phiFailureThreshold must be positive");
    this.localNode = checkNotNull(localNode, "localNode cannot be null");
    this.protocol = checkNotNull(protocol, "protocol cannot be null");
    this.peerProvider = checkNotNull(peerProvider, "peerProvider cannot be null");
    this.phiFailureThreshold = phiFailureThreshold;
    this.minSamples = minSamples;
    this.phiFactor = phiFactor;
    this.heartbeatFuture = heartbeatExecutor.scheduleAtFixedRate(
        this::heartbeat, heartbeatInterval.toMillis(), heartbeatInterval.toMillis(), TimeUnit.MILLISECONDS);
    protocol.registerHeartbeatListener(new HeartbeatMessageHandler());
  }

  private void updateState(T peer, FailureDetectionEvent.State newState) {
    FailureDetectionEvent.State currentState = nodeStates.get(peer);
    if (!Objects.equals(currentState, newState)) {
      nodeStates.put(peer, newState);
      post(new FailureDetectionEvent<T>(FailureDetectionEvent.Type.STATE_CHANGE, peer, currentState, newState));
    }
  }

  private void heartbeat() {
    try {
      Set<T> peers = peerProvider.get()
          .stream()
          .filter(peer -> !peer.equals(localNode))
          .collect(Collectors.toSet());
      FailureDetectionEvent.State state = nodeStates.get(localNode);
      HeartbeatMessage<T> heartbeat = new HeartbeatMessage<>(localNode, state);
      peers.forEach((node) -> {
        heartbeatToPeer(heartbeat, node);
        FailureDetectionEvent.State currentState = nodeStates.get(node.id());
        double phi = nodes.computeIfAbsent(node, n -> new PhiAccrualFailureDetector(minSamples, phiFactor)).phi();
        if (phi >= phiFailureThreshold) {
          if (currentState == FailureDetectionEvent.State.ACTIVE) {
            updateState(node, FailureDetectionEvent.State.INACTIVE);
          }
        } else {
          if (currentState == FailureDetectionEvent.State.INACTIVE) {
            updateState(node, FailureDetectionEvent.State.ACTIVE);
          }
        }
      });
    } catch (Exception e) {
      log.debug("Failed to send heartbeat", e);
    }
  }

  private void heartbeatToPeer(HeartbeatMessage<T> heartbeat, T peer) {
    protocol.heartbeat(peer, heartbeat).whenComplete((result, error) -> {
      if (error != null) {
        log.trace("Sending heartbeat to {} failed", peer, error);
      }
    });
  }

  private class HeartbeatMessageHandler implements Consumer<HeartbeatMessage<T>> {
    @Override
    public void accept(HeartbeatMessage<T> heartbeat) {
      nodes.computeIfAbsent(heartbeat.source(), n -> new PhiAccrualFailureDetector(minSamples, phiFactor)).report();
      updateState(heartbeat.source(), heartbeat.state());
    }
  }

  @Override
  public void close() {
    protocol.unregisterHeartbeatListener();
    heartbeatFuture.cancel(false);
  }

  /**
   * Phi-accrual failure detection service builder.
   *
   * @param <T> the node type
   */
  public static class Builder<T extends Identifier> implements FailureDetectionService.Builder<T> {
    private static final Duration DEFAULT_HEARTBEAT_INTERVAL = Duration.ofMillis(100);
    private static final int DEFAULT_PHI_FAILURE_THRESHOLD = 10;
    private static final int DEFAULT_MIN_SAMPLES = 25;
    private static final double DEFAULT_PHI_FACTOR = 1.0 / Math.log(10.0);

    private FailureDetectionProtocol<T> protocol;
    private T localNode;
    private Supplier<Collection<T>> peerProvider;
    private ScheduledExecutorService heartbeatExecutor;
    private Duration heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
    private int phiFailureThreshold = DEFAULT_PHI_FAILURE_THRESHOLD;
    private int minSamples = DEFAULT_MIN_SAMPLES;
    private double phiFactor = DEFAULT_PHI_FACTOR;

    /**
     * Sets the failure detection protocol.
     *
     * @param protocol the failure detection protocol
     * @return the failure detection service builder
     * @throws NullPointerException if the protocol is null
     */
    public Builder<T> withProtocol(FailureDetectionProtocol<T> protocol) {
      this.protocol = checkNotNull(protocol, "protocol cannot be null");
      return this;
    }

    /**
     * Sets the local node identifier.
     *
     * @param identifier the local identifier
     * @return the failure detection service builder
     * @throws NullPointerException if the identifier is null
     */
    public Builder<T> withLocalNode(T identifier) {
      this.localNode = identifier;
      return this;
    }

    /**
     * Sets the gossip peer provider function.
     *
     * @param peerProvider the gossip peer provider
     * @return the anti-entropy service builder
     * @throws NullPointerException if the peer provider is null
     */
    public Builder<T> withPeerProvider(Supplier<Collection<T>> peerProvider) {
      this.peerProvider = checkNotNull(peerProvider, "peerProvider cannot be null");
      return this;
    }

    /**
     * Sets the heartbeat executor.
     *
     * @param executor the heartbeat executor
     * @return the failure detection service builder
     * @throws NullPointerException if the heartbeat executor is null
     */
    public Builder<T> withHeartbeatExecutor(ScheduledExecutorService executor) {
      this.heartbeatExecutor = checkNotNull(executor, "executor cannot be null");
      return this;
    }

    /**
     * Sets the heartbeat interval.
     *
     * @param interval the heartbeat interval
     * @return the failure detection service builder
     * @throws NullPointerException if the heartbeat interval is null
     */
    public Builder<T> withHeartbeatInterval(Duration interval) {
      this.heartbeatInterval = checkNotNull(interval, "interval cannot be null");
      return this;
    }

    /**
     * Sets the phi failure threshold.
     *
     * @param failureThreshold the failure threshold
     * @return the failure detection service builder
     * @throws IllegalArgumentException if the failure threshold is not positive
     */
    public Builder<T> withPhiFailureThreshold(int failureThreshold) {
      checkArgument(failureThreshold > 0, "failureThreshold must be positive");
      this.phiFailureThreshold = failureThreshold;
      return this;
    }

    /**
     * Sets the minimum number of samples requires to compute phi.
     *
     * @param minSamples the minimum number of samples required to compute phi
     * @return the failure detection service builder
     * @throws IllegalArgumentException if the minimum number of samples is not positive
     */
    public Builder<T> withMinSamples(int minSamples) {
      checkArgument(minSamples > 0, "minSamples must be positive");
      this.minSamples = minSamples;
      return this;
    }

    /**
     * Sets the phi factor.
     *
     * @param phiFactor the phi factor
     * @return the failure detection service builder
     * @throws IllegalArgumentException if the phi factor is not positive
     */
    public Builder<T> withPhiFactor(double phiFactor) {
      checkArgument(phiFactor > 0, "phiFactor must be positive");
      this.phiFactor = phiFactor;
      return this;
    }

    @Override
    public FailureDetectionService<T> build() {
      return new PhiAccrualFailureDetectionService<>(
          protocol,
          localNode,
          peerProvider,
          heartbeatExecutor,
          heartbeatInterval,
          phiFailureThreshold,
          minSamples,
          phiFactor);
    }
  }
}
