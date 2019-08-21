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
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.socks.internal.SocksConfiguration;
import org.reaktivity.nukleus.socks.internal.SocksNukleus;
import org.reaktivity.nukleus.socks.internal.types.OctetsFW;
import org.reaktivity.nukleus.socks.internal.types.codec.*;
import org.reaktivity.nukleus.socks.internal.types.control.RouteFW;
import org.reaktivity.nukleus.socks.internal.types.stream.*;
import org.reaktivity.nukleus.stream.StreamFactory;
//import org.reaktivity.nukleus.socks.internal.types.StringFW;

//import java.util.function.Consumer;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

import static java.util.Objects.requireNonNull;

public final class SocksServerFactory implements StreamFactory
{
    private final RouteFW routeRO = new RouteFW();
    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();
    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();
    private final SignalFW signalRO = new SignalFW();
    private final SocksRequestFW socksRequestFW = new SocksRequestFW();
    private final SocksBeginExFW socksBeginExFW = new SocksBeginExFW();
    private final SocksReplyFW socksReplyFW = new SocksReplyFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();
    private final SignalFW.Builder signalRW = new SignalFW.Builder();
    private final OctetsFW.Builder octetsRw = new OctetsFW.Builder();
    private final SocksHandshakeReplyFW.Builder socksHandshakeReplyRw = new SocksHandshakeReplyFW.Builder();
    private final SocksReplyFW.Builder socksReplyRw = new SocksReplyFW.Builder();
    private final SocksAddressFW.Builder socksAddressRW = new SocksAddressFW.Builder();
    private final SocksBeginExFW.Builder socksBeginExRW = new SocksBeginExFW.Builder();

    private final SocksHandshakeRequestFW socksHandshakeRequestR0 = new SocksHandshakeRequestFW();
    private final SocksRequestFW socksRequestR0 = new SocksRequestFW();
    private final SocksReplyTypeFW socksReplyTypeR0 = new SocksReplyTypeFW();
    private final SocksAddressFW socksAddressFW = new SocksAddressFW();

    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final MutableDirectBuffer extBuffer;
    private final MutableDirectBuffer encodeBuffer;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;
    private final SocksConfiguration config;

    private final Long2ObjectHashMap<SocksServerStream> correlations;
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
            //TODO
            return true;
        };

        final RouteFW route = router.resolve(routeId, begin.authorization(), filter, this::wrapRoute);
        MessageConsumer newStream = null;

        if (route != null)
        {
            final SocksServer connection = new SocksServer(sender, routeId, initialId, replyId);
            newStream = connection::onNetwork;
        }
        return newStream;
    }

    private MessageConsumer newReplyStream(
        final BeginFW begin,
        final MessageConsumer sender)
    {
        final long replyId = begin.streamId();
        final SocksServerStream connect = correlations.remove(replyId);

        MessageConsumer newStream = null;
        if (connect != null)
        {
            newStream = connect::onApplication;
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
            this.decodeState = this::decodeCommandType;
        }

        private int decodeCommandType(
            SocksCommandType socksCommandType,
            DirectBuffer directBuffer,
            int offset,
            int length)
        {
            switch (socksCommandType)
            {
                case CONNECT:
                    final SocksRequestFW socksRequestFw = socksRequestFW.tryWrap(directBuffer, offset, length);
                    onSocksConnect(socksRequestFw);
                    break;
                case BIND:
                    //TODO
                    break;
                case UDP_ASSOCIATE:
                    //TODO
                    break;
                default:
                    //TODO
                    break;
            }
            return 0;
        }

        private void onNetwork(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            System.out.printf("Message MsgTypeId: %d \n", msgTypeId);
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
                case SignalFW.TYPE_ID:
                    final SignalFW signal = signalRO.wrap(buffer, index, index + length);
                    onNetworkSignal(signal);
                    break;
                default:
                    break;
            }
        }

        private void onSocksReply(
            String socksAddressFW,
            int port)
        {
            doSocksReply(socksAddressFW, port);
        }

        private void onNetworkData(
            DataFW data)
        {
            final OctetsFW payload = data.payload();
            initialBudget -= Math.max(data.length(), 0) + data.padding();
            if(initialBudget < 0)
            { // <=
                //doReply(supplyTraceId.getAsLong()); 02 not allowed by rulest
                //doNetworkReset(supplyTraceId.getAsLong());
            }
            else if (payload != null)
            {
                //onrequest switch the command
                decodeTraceId = data.trace();
                DirectBuffer buffer = payload.buffer();
                int offset = payload.offset();
                int limit = payload.limit();
                this.authorization = data.authorization();
                this.decodeTraceId = data.typeId();

                if (bufferSlot != BufferPool.NO_SLOT)
                {
                    MutableDirectBuffer decodeBuffer = bufferPool.buffer(bufferSlot);
                    decodeBuffer.putBytes(bufferSlotOffset, buffer, offset, limit - offset);
                    bufferSlotOffset += limit - offset;
                    buffer = decodeBuffer;
                    offset = 0;
                    limit = bufferSlot;
                }

                if (bufferSlot == BufferPool.NO_SLOT)
                {
                    bufferSlot = bufferPool.acquire(initialId);
                }

                if(payload.sizeof() == 3)
                {
                    final SocksHandshakeRequestFW handshakeRequest = socksHandshakeRequestR0.tryWrap(buffer, offset, limit);
                    onHandshakeRequest(handshakeRequest);
                }
                else if (payload.sizeof() == 18)
                {
                    final SocksRequestFW socksRequest = socksRequestR0.wrap(buffer, offset, limit);
                    onSocksRequest(socksRequest);
                }
            }
            else
            {
                //doReply error;
            }

        }

        private void onNetworkBegin(
            BeginFW begin)
        {
            doNetworkBegin(supplyTraceId.getAsLong());
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

        private void onSocksRequest(
            SocksRequestFW socksRequestFW)
        {
            if(socksRequestFW.version() != 5 && socksRequestFW.reserved() != 0)
            {
                //refuse the connection
                //TODO
            }
            SocksCommandType commandType = socksRequestFW.command().get();
            decodeCommandType(commandType,
                socksRequestFW.buffer(),
                socksRequestFW.offset(),
                socksRequestFW.limit());
        }

        private void onSocksConnect(
            SocksRequestFW socksRequest)
        {
            if(socksRequest == null)
            {
                return;
            }

            final MessagePredicate filter = (t, b, o, l) ->
            {
                if(t > 0 && t <= SignalFW.TYPE_ID && o < l)
                {
                    return true;
                }
                //System.out.printf("t: %s \n,b: %s\n, o: %s, %s \n", t, b, o, l);
                return false;
            };

            final RouteFW route = router.resolve(routeId, this.authorization, filter, wrapRoute);
            if(route != null)
            {
                final long newRouteId = route.correlationId();
                final long newInitialId = supplyInitialId.applyAsLong(newRouteId);
                final long newReplyId = supplyReplyId.applyAsLong(newInitialId);

                final MessageConsumer newTarget = router.supplyReceiver(newInitialId);
                final SocksServerStream socksServerStream = new SocksServerStream(this, newTarget,
                    newRouteId, newInitialId, newReplyId);

                SocksBeginExFW socksBeginEx = socksBeginExRW.wrap(extBuffer,
                                                            0,
                                                            extBuffer.capacity())
                                                                     .typeId(socksTypeId)
                                                                     .address(socksRequest.address().domainName())
                                                                     .port(socksRequest.port())
                                                                     .build();
                socksServerStream.doApplicationBeginEx(newTarget, newRouteId, newInitialId, decodeTraceId,
                socksBeginEx.buffer(), socksBeginEx.offset(), socksBeginEx.limit());
                correlations.put(newReplyId, socksServerStream);
            }
            //SocksAddressFW socksAddressFW = socksRequest.address();
        }

        private void onNetworkReset(
            ResetFW reset)
        {
            final long traceId = reset.trace();
            doReset(traceId);
        }

        private void onNetworkSignal(
            SignalFW signal)
        {
            final long traceId = signal.trace();
            doSignal(traceId);
        }

        private void onHandshakeRequest(
            SocksHandshakeRequestFW onHandshakeRequest
        )
        {
            if(onHandshakeRequest.version() != 5)
            {
                //TODO
            }
            int nmethods = onHandshakeRequest.nmethods();
            OctetsFW method = onHandshakeRequest.methods();
            if(nmethods != method.limit() - method.offset())
            {
                //TODO refuse connection
            } else
            {
                onHandshakeReply(method);
            }
            //int numMethod = method.buffer().getInt(method.offset());
        }

        private void onHandshakeReply(
            OctetsFW method)
        {
            int methodNumber = method.buffer().getByte(method.offset());
            SocksAuthenticationMethod socksAuthenticationMethod = SocksAuthenticationMethod.valueOf((short) methodNumber);
            switch (socksAuthenticationMethod)
            {
                case NO_AUTHENTICATION_REQUIRED:
                    doNoAuthenticationRequired(methodNumber);
                    break;
                case GSSAPI:
                    //todo
                    break;
                case USERNAME_PASSWORD:
                    //todo
                    break;
                case TO_X7F_IANA_ASSIGNED:
                    //todo
                    break;
                case TO_XFE_RESERVED_FOR_PRIVATE_METHODS:
                    //todo
                    break;
                default:
                    break;
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

            network.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
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

        private void doReset(
            long traceId)
        {
            final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(initialId)
                .trace(traceId)
                .build();

            network.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
        }

        private void doSignal(
            long traceId)
        {
            final SignalFW signal = signalRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(initialId)
                .trace(traceId)
                .build();

            network.accept(signal.typeId(), signal.buffer(), signal.offset(), signal.sizeof());
        }

        private void doNoAuthenticationRequired(
            int method)
        {
            SocksHandshakeReplyFW socksHandshakeReplyFW = socksHandshakeReplyRw.wrap(writeBuffer,
                DataFW.FIELD_OFFSET_PAYLOAD,
                writeBuffer.capacity())
                .version(5)
                .method(method)
                .build();

            doNetworkData(socksHandshakeReplyFW.buffer(), socksHandshakeReplyFW.offset(), socksHandshakeReplyFW.sizeof());
        }

        //private void doSocksBegin

        private void doSocksConnect(
            SocksRequestFW socksRequest)
        {
            //doApplicationBegin(socksRequest);
            SocksAddressFW socksAddressFW = socksRequest.address();
            //onSocksReply((byte) 0, socksAddressFW, socksRequestFW.port());
        }

        private void doSocksReply(
            String address,
            int port)
        {
            byte[] ipv4Address = lookupName(address).getAddress();

            SocksReplyFW socksReply = socksReplyRw.wrap(writeBuffer,
                DataFW.FIELD_OFFSET_PAYLOAD,
                writeBuffer.capacity())
                .version(5)
                .type(t -> t.set(SocksReplyType.SUCCEEDED))
                .reserved(0)
                .address(a->a.ipv4Address(i->i.set(ipv4Address)))
                .port(port)
                .build();
            doNetworkData(socksReply.buffer(), socksReply.offset(), socksReply.sizeof());
        }
    }

    private final class SocksServerStream
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

        SocksServerStream(
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
            System.out.printf("Application MsgTypeId: %d \n", msgTypeId);
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
                default:
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

        private void onApplicationSignal(
            SignalFW singal)
        {
            //TODO
        }

        private void onApplicationBegin(
            BeginFW begin)
        {
            OctetsFW extension = begin.extension();
            SocksBeginExFW socksBeginEx = extension.get(socksBeginExFW::wrap);

            String address = socksBeginEx.address().asString();
            int port = socksBeginEx.port();


            //SocksServer receiver = new SocksServer(new, begin.routeId(), this.receiver.initialId ,begin.typeId());

            /*byte[] ipv4Address = lookupName(address).getAddress();
            SocksAddressFW socksAddress = socksAddressRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                                        .ipv4Address(t->t.set(ipv4Address))
                                                        .build();*/


            this.receiver.doSocksReply(address, port);
        }

        private void onApplicationData(
            DataFW data)
        {
            final OctetsFW payload = data.payload();
            initialBudget -= Math.max(data.length(), 0) + data.padding();
            //System.out.printf("543 payload: %s\n", payload);

        }

        private void onApplicationEnd(
            EndFW end)
        {
            final long traceId = end.trace();
            doApplicationEnd(traceId);
        }

        private void doApplicationBeginEx(
            MessageConsumer target,
            long routeId,
            long streamId,
            long traceId,
            DirectBuffer buffer,
            int offset,
            int length)
        {

            final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .extension(buffer, offset, length)
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
                .streamId(replyId)
                .trace(supplyTraceId.getAsLong())
                .groupId(0)
                .padding(replyPadding)
                .payload(buffer, offset, sizeOf)
                .build();
            application.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
            //router.setThrottle(replyId, this::onApplication);
        }

        private void doApplicationEnd(
            long traceId)
        {
            final EndFW end = endRW.wrap(extBuffer, 0, extBuffer.capacity())
                .routeId(routeId)
                .streamId(replyId)
                .trace(traceId)
                .build();
            this.receiver.network.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
        }
    }

    private static InetAddress lookupName(
        String host)
    {
        InetAddress address = null;

        try
        {
            address = InetAddress.getByName(host);
        }
        catch (UnknownHostException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return address;
    }

    @FunctionalInterface
    private interface DecoderState
    {
        int decode(SocksCommandType commandType, DirectBuffer buffer, int offset, int length);
    }
}