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
package org.reaktivity.nukleus.socks.internal.stream.server;

import static java.util.Objects.requireNonNull;

import java.util.function.LongSupplier;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.socks.internal.stream.Correlation;
import org.reaktivity.nukleus.socks.internal.stream.SocksNegotiationFW;
import org.reaktivity.nukleus.socks.internal.types.OctetsFW;
import org.reaktivity.nukleus.socks.internal.types.control.RouteFW;
import org.reaktivity.nukleus.socks.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.socks.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.socks.internal.types.stream.DataFW;
import org.reaktivity.nukleus.socks.internal.types.stream.EndFW;
import org.reaktivity.nukleus.socks.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.socks.internal.types.stream.WindowFW;

public class StreamContext
{
    final RouteFW routeRO = new RouteFW();
    final BeginFW beginRO = new BeginFW();
    final DataFW dataRO = new DataFW();
    final EndFW endRO = new EndFW();
    final AbortFW abortRO = new AbortFW();

    final BeginFW.Builder beginRW = new BeginFW.Builder();
    final DataFW.Builder dataRW = new DataFW.Builder();
    final EndFW.Builder endRW = new EndFW.Builder();
    final AbortFW.Builder abortRW = new AbortFW.Builder();

    final WindowFW windowRO = new WindowFW();
    final ResetFW resetRO = new ResetFW();

    final WindowFW.Builder windowRW = new WindowFW.Builder();
    final ResetFW.Builder resetRW = new ResetFW.Builder();

    final OctetsFW octetsRO = new OctetsFW();

    final RouteManager router;
    final MutableDirectBuffer writeBuffer;
    final BufferPool bufferPool;
    final LongSupplier supplyStreamId;
    final LongSupplier supplyCorrelationId;

    final Long2ObjectHashMap<Correlation> correlations;
    final MessageFunction<RouteFW> wrapRoute;

    final SocksNegotiationFW socksNegotiationRO = new SocksNegotiationFW();
    final SocksNegotiationFW socksNegotiationRW = new SocksNegotiationFW(); // TODO add .Builder (type + declaration)

    public StreamContext(
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
        System.out.println("Got the StreamContext setup ....");
    }
}
