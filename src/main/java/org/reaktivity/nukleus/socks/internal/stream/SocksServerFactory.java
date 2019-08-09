/**
 * Copyright 2016-2019 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.socks.internal.stream;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.socks.internal.SocksConfiguration;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.agrona.DirectBuffer;

import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;

import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;

import org.reaktivity.nukleus.socks.internal.types.control.RouteFW;

public final class SocksServerFactory implements StreamFactory
{
    private final RouteFW routeRO = new RouteFW();

    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final MutableDirectBuffer encodeBuffer;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;
    private final SocksConfiguration config;

    private final BufferPool bufferPool;

    private final MessageFunction<RouteFW> wrapRoute;
    public SocksServerFactory(
        SocksConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTraceId)
    {
        this.config = config;
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.bufferPool = bufferPool;
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.supplyTraceId = requireNonNull(supplyTraceId);
        this.wrapRoute = this::wrapRoute;
        this.encodeBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
    }

    private RouteFW wrapRoute(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
    {
        return routeRO.wrap(buffer, index, index + length);
    }

    @Override
    public MessageConsumer newStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length,
        MessageConsumer throttle)
    {
        return null;
    }

    private final class SocksServer {
        private final MessageConsumer network;
        private final long routeId;
        private final long initialId;
        private final long replyId;

        private int initialBudget;
        private int initialPadding;
        private int replyBudget;
        private int replyPadding;

        private long decodeTraceId;
        private DecoderState decodeState;
        private int slotIndex = NO_SLOT;
        private int slotLimit;

    }

    @FunctionalInterface
    private interface DecoderState
    {
        int decode(DirectBuffer buffer, int offset, int length);
    }

}