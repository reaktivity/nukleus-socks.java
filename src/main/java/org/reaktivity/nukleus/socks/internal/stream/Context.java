/**
 * Copyright 2016-2017 The Reaktivity Project
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

import static java.util.Objects.requireNonNull;

import java.util.function.LongSupplier;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.socks.internal.stream.types.SocksCommandRequestFW;
import org.reaktivity.nukleus.socks.internal.stream.types.SocksCommandResponseFW;
import org.reaktivity.nukleus.socks.internal.stream.types.SocksNegotiationRequestFW;
import org.reaktivity.nukleus.socks.internal.stream.types.SocksNegotiationResponseFW;
import org.reaktivity.nukleus.socks.internal.types.OctetsFW;
import org.reaktivity.nukleus.socks.internal.types.control.RouteFW;
import org.reaktivity.nukleus.socks.internal.types.control.SocksRouteExFW;
import org.reaktivity.nukleus.socks.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.socks.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.socks.internal.types.stream.DataFW;
import org.reaktivity.nukleus.socks.internal.types.stream.EndFW;
import org.reaktivity.nukleus.socks.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.socks.internal.types.stream.WindowFW;
import org.reaktivity.specification.socks.internal.types.stream.TcpBeginExFW;

public class Context
{
    public final RouteFW routeRO = new RouteFW();
    public final BeginFW beginRO = new BeginFW();
    public final DataFW dataRO = new DataFW();
    public final EndFW endRO = new EndFW();
    public final AbortFW abortRO = new AbortFW();

    public final TcpBeginExFW tcpBeginExRO = new TcpBeginExFW();

    public final BeginFW.Builder beginRW = new BeginFW.Builder();
    public final DataFW.Builder dataRW = new DataFW.Builder();
    public final EndFW.Builder endRW = new EndFW.Builder();
    public final AbortFW.Builder abortRW = new AbortFW.Builder();

    public final WindowFW windowRO = new WindowFW();
    public final ResetFW resetRO = new ResetFW();

    public final WindowFW.Builder windowRW = new WindowFW.Builder();
    public final ResetFW.Builder resetRW = new ResetFW.Builder();

    public final OctetsFW octetsRO = new OctetsFW();

    public final RouteManager router;
    public final MutableDirectBuffer writeBuffer; // TODO consider using 2 buffers: S -> T and T -> S
                                                  // TODO or confirm there cannot be a race condition

    public final BufferPool bufferPool;
    public final LongSupplier supplyStreamId;
    public final LongSupplier supplyCorrelationId;

    public final Long2ObjectHashMap<Correlation> correlations;
    public final MessageFunction<RouteFW> wrapRoute;

    public final SocksRouteExFW routeExRO = new SocksRouteExFW();

    // Socks protocol flyweights
    public final SocksNegotiationRequestFW socksNegotiationRequestRO = new SocksNegotiationRequestFW();
    public final SocksNegotiationRequestFW.Builder socksNegotiationRequestRW = new SocksNegotiationRequestFW.Builder();

    public final SocksNegotiationResponseFW socksNegotiationResponseRO = new SocksNegotiationResponseFW();
    public final SocksNegotiationResponseFW.Builder socksNegotiationResponseRW = new SocksNegotiationResponseFW.Builder();

    public final SocksCommandRequestFW socksConnectionRequestRO = new SocksCommandRequestFW();
    public final SocksCommandRequestFW.Builder socksConnectionRequestRW = new SocksCommandRequestFW.Builder();

    public final SocksCommandResponseFW socksConnectionResponseRO = new SocksCommandResponseFW();
    public final SocksCommandResponseFW.Builder socksConnectionResponseRW = new SocksCommandResponseFW.Builder();

    public Context(
        Configuration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongSupplier supplyStreamId,
        LongSupplier supplyCorrelationId,
        Long2ObjectHashMap<Correlation> correlations,
        MessageFunction<RouteFW> wrapRoute)
    {
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.bufferPool = requireNonNull(bufferPool);
        this.supplyStreamId = requireNonNull(supplyStreamId);
        this.supplyCorrelationId = requireNonNull(supplyCorrelationId);
        this.correlations = requireNonNull(correlations);
        this.wrapRoute = requireNonNull(wrapRoute);
    }
}
