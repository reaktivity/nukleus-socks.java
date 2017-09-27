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
package org.reaktivity.nukleus.socks.internal.stream.client;

import java.util.function.LongSupplier;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.socks.internal.stream.Correlation;
import org.reaktivity.nukleus.socks.internal.stream.Context;
import org.reaktivity.nukleus.socks.internal.types.control.RouteFW;
import org.reaktivity.nukleus.socks.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.stream.StreamFactory;

public class ClientStreamFactory implements StreamFactory
{

    private final Context context;

    public ClientStreamFactory(
        Configuration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongSupplier supplyStreamId,
        LongSupplier supplyCorrelationId,
        Long2ObjectHashMap<Correlation> correlations)
    {
        System.out.println(this.getClass() + " init");
        this.context =
            new Context(
                config,
                router,
                writeBuffer,
                bufferPool,
                supplyStreamId,
                supplyCorrelationId,
                correlations,
                this::wrapRoute);
    }

    @Override
    public MessageConsumer newStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length,
        MessageConsumer throttle)
    {
        System.out.println(this.getClass() + "newStream");

        final BeginFW begin = context.beginRO.wrap(buffer, index, index + length);
        final long sourceRef = begin.sourceRef();

        if (sourceRef == 0L)
        {
            System.out.println("connect reply stream");
            return newConnectReplyStream(begin, throttle);
        }
        else
        {
            System.out.println("accept stream");
            return newAcceptStream(begin, throttle);
        }
    }

    private MessageConsumer newAcceptStream(
        final BeginFW begin,
        final MessageConsumer acceptThrottle)
    {
        final long acceptSourceRef = begin.sourceRef();
        final String acceptSourceName = begin.source().asString();
        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = context.routeRO.wrap(b, o, l);
            return acceptSourceRef == route.sourceRef() && acceptSourceName.equals(route.source().asString());
        };
        final RouteFW route = context.router.resolve(filter, this::wrapRoute);
        if (route != null)
        {
            // FIXME will wrap again the BeginFW in the AcceptStreamHandler
            return new AcceptStreamHandler(
                context,
                acceptThrottle,
                begin.streamId(),
                acceptSourceRef,
                acceptSourceName,
                begin.correlationId()
            )::handleStream;
        }
        return null;
    }

    private MessageConsumer newConnectReplyStream(
        final BeginFW begin,
        final MessageConsumer connectReplyThrottle)
    {
        return new ConnectReplyStreamHandler(context, connectReplyThrottle, begin.streamId())::handleStream;
    }

    private RouteFW wrapRoute(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        System.out.println("WrapRoute");
        return context.routeRO.wrap(buffer, index, index + length);
    }
}