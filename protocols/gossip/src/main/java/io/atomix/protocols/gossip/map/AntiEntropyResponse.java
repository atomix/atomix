// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.gossip.map;

/**
 * Status of anti-entropy exchange, returned by the receiver.
 *
 */
public enum AntiEntropyResponse {
    /**
     * Signifies a successfully processed anti-entropy message.
     */
    PROCESSED,

    /**
     * Signifies a unexpected failure during anti-entropy message processing.
     */
    FAILED,

    /**
     * Signifies a ignored anti-entropy message, potentially due to the receiver operating under high load.
     */
    IGNORED
}
