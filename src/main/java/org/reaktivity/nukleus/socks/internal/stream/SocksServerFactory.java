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

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;

import org.reaktivity.nukleus.socks.internal.types.codec.SocksCommandReplyFW;
import org.reaktivity.nukleus.socks.internal.types.codec.SocksHandshakeReplyFW;
import org.reaktivity.nukleus.socks.internal.types.codec.SocksHandshakeRequestFW;
import org.reaktivity.nukleus.socks.internal.types.codec.SocksCommandRequestFW;
import org.reaktivity.nukleus.socks.internal.types.codec.SocksAuthenticationMethod;
import org.reaktivity.nukleus.socks.internal.types.codec.SocksCommandReplyType;
import org.reaktivity.nukleus.socks.internal.types.codec.SocksAddressFW;

import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.socks.internal.SocksConfiguration;
import org.reaktivity.nukleus.socks.internal.SocksNukleus;
import org.reaktivity.nukleus.socks.internal.types.OctetsFW;
import org.reaktivity.nukleus.socks.internal.types.StringFW;
import org.reaktivity.nukleus.socks.internal.types.control.RouteFW;
import org.reaktivity.nukleus.socks.internal.types.control.SocksRouteExFW;
import org.reaktivity.nukleus.socks.internal.types.stream.SocksBeginExFW;
import org.reaktivity.nukleus.socks.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.socks.internal.types.stream.EndFW;
import org.reaktivity.nukleus.socks.internal.types.stream.DataFW;
import org.reaktivity.nukleus.socks.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.socks.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.socks.internal.types.stream.ResetFW;

import org.reaktivity.nukleus.stream.StreamFactory;

import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.socks.internal.types.codec.SocksAuthenticationMethod.NO_AUTHENTICATION_REQUIRED;
import static org.reaktivity.nukleus.socks.internal.types.codec.SocksAuthenticationMethod.NO_ACCEPTABLE_METHODS;

public final class SocksServerFactory implements StreamFactory
{
    private final RouteFW routeRO = new RouteFW();
    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();
    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();
    private final SocksBeginExFW socksBeginExRO = new SocksBeginExFW();
    private final SocksHandshakeRequestFW handshakeRequestRO = new SocksHandshakeRequestFW();
    private final SocksCommandRequestFW socksCommandRequestRO = new SocksCommandRequestFW();
    private final SocksAddressFW socksAddressRO = new SocksAddressFW();
    private final OctetsFW octetsRO = new OctetsFW();
    private final StringFW stringRO = new StringFW();
    private final SocksRouteExFW socksRouteRO = new SocksRouteExFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();
    private final SocksHandshakeReplyFW.Builder handshakeReplyRW = new SocksHandshakeReplyFW.Builder();
    private final SocksCommandReplyFW.Builder socksCommandReplyRW = new SocksCommandReplyFW.Builder();
    private final SocksBeginExFW.Builder socksBeginExRW = new SocksBeginExFW.Builder();
    private final OctetsFW.Builder octetsRW = new OctetsFW.Builder();
    private final StringFW.Builder stringRW = new StringFW.Builder();
    private final SocksRouteExFW.Builder socksRouteRW = new SocksRouteExFW.Builder();

    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final MutableDirectBuffer extBuffer;
    private final MutableDirectBuffer encodeBuffer;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;
    private final SocksConfiguration config;

    private final Long2ObjectHashMap<SocksConnectStream> correlations;
    private final MessageFunction<RouteFW> wrapRoute;

    private final BufferPool bufferPool;
    private final int socksTypeId;

    public SocksServerFactory(
        SocksConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTraceId,
        ToIntFunction<String> supplyTypeId)
    {
        this.config = config;
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.extBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.bufferPool = bufferPool;
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.supplyTraceId = requireNonNull(supplyTraceId);
        this.correlations = new Long2ObjectHashMap<>();
        this.wrapRoute = this::wrapRoute;
        this.encodeBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.socksTypeId = supplyTypeId.applyAsInt(SocksNukleus.NAME);
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
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long streamId = begin.streamId();

        MessageConsumer newStream = null;

        if ((streamId & 0x0000_0000_0000_0001L) != 0L)
        {
            newStream = newInitialStream(begin, throttle);
        }
        else
        {
            newStream = newReplyStream(begin, throttle);
        }
        return newStream;
    }

    private MessageConsumer newInitialStream(
        final BeginFW begin,
        final MessageConsumer sender)
    {
        final long routeId = begin.routeId();
        final long initialId = begin.streamId();
        final long replyId = supplyReplyId.applyAsLong(initialId);

        final MessagePredicate filter = (t, b, o, l) ->
        {
            return true;
        };

        final RouteFW route = router.resolve(routeId, begin.authorization(), filter, this::wrapRoute);
        MessageConsumer newStream = null;

        if (route != null)
        {
            final SocksServer server = new SocksServer(sender, routeId, initialId, replyId);
            newStream = server::onNetwork;
        }
        return newStream;
    }

    private MessageConsumer newReplyStream(
        final BeginFW begin,
        final MessageConsumer sender)
    {
        final long replyId = begin.streamId();
        final SocksConnectStream serverStream = correlations.remove(replyId);

        MessageConsumer newStream = null;
        if (serverStream != null)
        {
            newStream = serverStream::onApplication;
        }
        return newStream;
    }

    private final class SocksServer
    {
        private final MessageConsumer network;
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private long authorization;

        private int initialBudget;
        private int initialPadding;
        private int replyBudget;
        private int replyPadding;

        private long decodeTraceId;
        private DecoderState decodeState;
        private int bufferSlot = BufferPool.NO_SLOT;
        private int bufferSlotOffset;

        private SocksServer(
            MessageConsumer network,
            long routeId,
            long initialId,
            long replyId)
        {
            this.network = network;
            this.routeId = routeId;
            this.initialId = initialId;
            this.replyId = replyId;
            this.decodeState = this::decodeHandshake;
        }

        private int decodeHandshake(
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            SocksHandshakeRequestFW handshakeRequest = handshakeRequestRO.tryWrap(buffer, offset, limit);
            int progress = offset;
            if (handshakeRequest != null)
            {
                progress = handshakeRequest.limit();
                onHandshakeRequest(handshakeRequest);
            }
            return progress;
        }

        private int decodeCommand(
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            SocksCommandRequestFW commandRequest = socksCommandRequestRO.tryWrap(buffer, offset, limit);
            int progress = offset;
            if (commandRequest != null)
            {
                progress = commandRequest.limit();
                onSocksConnect(commandRequest);
            }
            return progress;
        }

        private void onNetwork(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
                case BeginFW.TYPE_ID:
                    final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                    onNetworkBegin(begin);
                    break;
                case DataFW.TYPE_ID:
                    final DataFW data = dataRO.wrap(buffer, index, index + length);
                    onNetworkData(data);
                    break;
                case EndFW.TYPE_ID:
                    final EndFW end = endRO.wrap(buffer, index, index + length);
                    onNetworkEnd(end);
                    break;
                case AbortFW.TYPE_ID:
                    final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                    onNetworkAbort(abort);
                    break;
                case WindowFW.TYPE_ID:
                    final WindowFW window = windowRO.wrap(buffer, index, index + length);
                    onNetworkWindow(window);
                    break;
                case ResetFW.TYPE_ID:
                    final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                    onNetworkReset(reset);
                    break;
            }
        }

        private void onNetworkData(
            DataFW data)
        {
            final OctetsFW payload = data.payload();
            initialBudget -= Math.max(data.length(), 0) + data.padding();
            if (initialBudget < 0)
            {
                doNetworkReset(data.trace());
            }
            else if (payload != null)
            {
                decodeTraceId = data.trace();
                DirectBuffer buffer = payload.buffer();
                int offset = payload.offset();
                int limit = payload.limit();
                authorization = data.authorization();
                decodeTraceId = data.typeId();

                if (bufferSlot != BufferPool.NO_SLOT)
                {
                    MutableDirectBuffer decodeBuffer = bufferPool.buffer(bufferSlot);
                    decodeBuffer.putBytes(bufferSlotOffset, buffer, offset, limit - offset);
                    bufferSlotOffset += limit - offset;
                    buffer = decodeBuffer;
                    offset = 0;
                    limit = bufferSlot;
                }

                DecoderState previous = null;
                while (offset <= limit && previous != decodeState)
                {
                    previous = decodeState;
                    offset += decodeState.decode(buffer, offset, limit);
                }
            }
        }

        private void onNetworkBegin(
            BeginFW begin)
        {
            doNetworkBegin(supplyTraceId.getAsLong());
        }

        private void onNetworkReset(
            ResetFW reset)
        {
            final long traceId = reset.trace();
            doNetworkReset(traceId);
        }

        private void onNetworkEnd(
            EndFW end)
        {
            final long traceId = end.trace();
            doNetworkEnd(traceId);
        }

        private void onNetworkAbort(
            AbortFW abort)
        {
            final long traceId = abort.trace();
            doNetworkAbort(traceId);
        }

        private void onNetworkWindow(
            WindowFW window)
        {
            final int replyCredit = window.credit();

            replyBudget += replyCredit;
            replyPadding += window.padding();

            final int initialCredit = bufferPool.slotCapacity() - initialBudget;
            doNetworkWindow(supplyTraceId.getAsLong(), initialCredit);
        }

        private void onSocksConnect(
            SocksCommandRequestFW socksConnectRequest)
        {
            final MessagePredicate filter = (t, b, o, l) ->
            {
                final RouteFW route = routeRO.wrap(b, o, o + l);
                final OctetsFW extension = route.extension();
                final SocksRouteExFW socksRoute = extension.get(socksRouteRO::wrap);
                final SocksAddressFW requestAddress = socksConnectRequest.address();
                final SocksAddressFW routedAddress = socksAddressRO.wrap(socksRoute.address().buffer(),
                                                                         socksRoute.address().offset(),
                                                                         socksRoute.address().limit());

                return socksRoute.port() == socksConnectRequest.port() &&
                    requestAddress.equals(routedAddress);
            };

            final RouteFW route = router.resolve(routeId, authorization, filter, wrapRoute);
            if (route != null)
            {
                final long newRouteId = route.correlationId();
                final long newInitialId = supplyInitialId.applyAsLong(newRouteId);
                final long newReplyId = supplyReplyId.applyAsLong(newInitialId);

                final MessageConsumer newTarget = router.supplyReceiver(newInitialId);
                final SocksConnectStream socksConnectStream = new SocksConnectStream(this, newTarget,
                    newRouteId, newInitialId, newReplyId);

                SocksAddressFW requestAddress = socksConnectRequest.address();

                socksConnectStream.doApplicationBegin(newTarget,
                                                      decodeTraceId,
                                                      requestAddress.domainName(),
                                                      socksConnectRequest.port());

                correlations.put(newReplyId, socksConnectStream);
            }
        }

        private void onHandshakeRequest(
            SocksHandshakeRequestFW handshakeRequest)
        {
            OctetsFW methods = handshakeRequest.methods();
            if (handshakeRequest.version() != 5)
            {
                doNetworkEnd(supplyTraceId.getAsLong());
            }
            else if (!hasAuthenticationMethod(methods, NO_AUTHENTICATION_REQUIRED))
            {
                doSocksHandshakeReply(NO_ACCEPTABLE_METHODS);
                doNetworkEnd(supplyTraceId.getAsLong());
            }
            else
            {
                doSocksHandshakeReply(NO_AUTHENTICATION_REQUIRED);
                decodeState = this::decodeCommand;
            }
        }

        private void doNetworkBegin(
            long traceId)
        {
            final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                         .routeId(routeId)
                                         .streamId(replyId)
                                         .trace(traceId)
                                         .build();

            network.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
            router.setThrottle(replyId, this::onNetwork);
        }

        private void doNetworkData(
            DirectBuffer buffer,
            int offset,
            int sizeOf)
        {
            final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                      .routeId(routeId)
                                      .streamId(replyId)
                                      .trace(supplyTraceId.getAsLong())
                                      .groupId(0)
                                      .padding(replyPadding)
                                      .payload(buffer, offset, sizeOf)
                                      .build();

            network.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
            router.setThrottle(replyId, this::onNetwork);
        }

        private void doNetworkEnd(
            long traceId)
        {
            final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                   .routeId(routeId)
                                   .streamId(replyId)
                                   .trace(traceId)
                                   .build();

            network.accept(end.typeId(), end.buffer(), end.offset(), end.limit());
        }

        private void doNetworkAbort(
            long traceId)
        {
            final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                         .routeId(routeId)
                                         .streamId(replyId)
                                         .trace(traceId)
                                         .build();

            network.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
        }

        private void doNetworkWindow(
            long traceId,
            int initialCredit)
        {
            if (initialCredit > 0)
            {
                initialBudget += initialCredit;

                final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                                .routeId(routeId)
                                                .streamId(initialId)
                                                .trace(traceId)
                                                .credit(initialCredit)
                                                .padding(initialPadding)
                                                .groupId(0)
                                                .build();

                network.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
            }
        }

        private void doNetworkReset(
            long traceId)
        {
            final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                         .routeId(routeId)
                                         .streamId(initialId)
                                         .trace(traceId)
                                         .build();

            network.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
        }

        private void doSocksHandshakeReply(
            SocksAuthenticationMethod method)
        {
            SocksHandshakeReplyFW handshakeReply = handshakeReplyRW.wrap(writeBuffer,
                                                                         DataFW.FIELD_OFFSET_PAYLOAD,
                                                                         writeBuffer.capacity())
                                                                   .version(5)
                                                                   .method(method.value())
                                                                   .build();

            doNetworkData(handshakeReply.buffer(), handshakeReply.offset(), handshakeReply.sizeof());
        }

        private void doSocksCommandReply(
            SocksAddressFW address,
            int port)
        {
            SocksCommandReplyFW socksCommandReply = socksCommandReplyRW.wrap(writeBuffer,
                                                                             DataFW.FIELD_OFFSET_PAYLOAD,
                                                                             writeBuffer.capacity())
                                                                       .version(5)
                                                                       .type(t -> t.set(SocksCommandReplyType.SUCCEEDED))
                                                                       .reserved(0)
                                                                       .address(s->s.ipv4Address(i ->
                                                                           i.set(address.ipv4Address())))
                                                                       .port(port)
                                                                       .build();

            doNetworkData(socksCommandReply.buffer(), socksCommandReply.offset(), socksCommandReply.sizeof());
        }
    }

    private final class SocksConnectStream
    {
        private final SocksServer receiver;
        private final MessageConsumer application;
        private long routeId;
        private long initialId;
        private long replyId;

        private int initialBudget;
        private int initialPadding;
        private int replyBudget;
        private int replyPadding;

        private long decodeTraceId;
        private DecoderState decodeState;
        private int bufferSlot = BufferPool.NO_SLOT;
        private int bufferSlotOffset;

        SocksConnectStream(
            SocksServer network,
            MessageConsumer application,
            long routeId,
            long initialId,
            long replyId)
        {
            this.receiver = network;
            this.application = application;
            this.routeId = routeId;
            this.initialId = initialId;
            this.replyId = replyId;
        }

        public void onApplication(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch(msgTypeId)
            {
                case BeginFW.TYPE_ID:
                    final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                    onApplicationBegin(begin);
                    break;
                case DataFW.TYPE_ID:
                    final DataFW data = dataRO.wrap(buffer, index, index + length);
                    onApplicationData(data);
                    break;
                case EndFW.TYPE_ID:
                    final EndFW end = endRO.wrap(buffer, index, index + length);
                    onApplicationEnd(end);
                    break;
                case WindowFW.TYPE_ID:
                    final WindowFW window = windowRO.wrap(buffer, index, index + length);
                    onApplicationWindow(window);
                    break;
                case ResetFW.TYPE_ID:
                    final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                    onApplicationReset(reset);
                    break;
            }
        }

        private void onApplicationWindow(
            WindowFW window)
        {
            //TODO
        }

        private void onApplicationReset(
            ResetFW reset)
        {
            //TODO
        }

        private void onApplicationBegin(
            BeginFW begin)
        {

            OctetsFW extension = begin.extension();
            SocksBeginExFW socksBeginEx = extension.get(socksBeginExRO::wrap);

            SocksAddressFW replyAddress = socksAddressRO.wrap(socksBeginEx.address().buffer(),
                socksBeginEx.address().offset(),
                socksBeginEx.address().limit());
            int port = socksBeginEx.port();

            receiver.doSocksCommandReply(replyAddress, port);

        }

        private void onApplicationData(
            DataFW data)
        {
            final OctetsFW payload = data.payload();
            //TODO
        }

        private void onApplicationEnd(
            EndFW end)
        {
            final long traceId = end.trace();
            doApplicationEnd(traceId);
        }

        private void doApplicationBegin(
            MessageConsumer target,
            long traceId,
            StringFW address,
            int port)
        {
            SocksBeginExFW socksBeginEx = socksBeginExRW.wrap(extBuffer, 0, extBuffer.capacity())
                                                        .typeId(socksTypeId)
                                                        .address(t->t.domainName(address.asString()))
                                                        .port(port)
                                                        .build();

            final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                         .routeId(routeId)
                                         .streamId(initialId)
                                         .trace(traceId)
                                         .extension(socksBeginEx.buffer(), socksBeginEx.offset(), socksBeginEx.sizeof())
                                         .build();

            target.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.limit());
        }

        private void doApplicationData(
            DirectBuffer buffer,
            int offset,
            int sizeOf)
        {
            final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                      .routeId(routeId)
                                      .streamId(initialId)
                                      .trace(supplyTraceId.getAsLong())
                                      .groupId(0)
                                      .padding(replyPadding)
                                      .payload(buffer, offset, sizeOf)
                                      .build();

            application.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
        }

        private void doApplicationEnd(
            long traceId)
        {
            final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                   .routeId(routeId)
                                   .streamId(initialId)
                                   .trace(traceId)
                                   .build();

            application.accept(end.typeId(), end.buffer(), end.offset(), end.limit());
        }
    }

    private static boolean hasAuthenticationMethod(
        OctetsFW methods,
        SocksAuthenticationMethod method)
    {
        boolean hasMethod = false;
        for (int i = methods.offset(); i < methods.limit(); i++)
        {
            if (method.value() == (methods.buffer().getByte(i) & 0xff))
            {
                hasMethod = true;
                break;
            }
        }

        return hasMethod;
    }

    @FunctionalInterface
    private interface DecoderState
    {
        int decode(DirectBuffer buffer, int offset, int length);
    }
}
