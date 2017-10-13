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

import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;

import java.util.Optional;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.socks.internal.metadata.Signal;
import org.reaktivity.nukleus.socks.internal.metadata.State;
import org.reaktivity.nukleus.socks.internal.stream.AcceptTransitionListener;
import org.reaktivity.nukleus.socks.internal.stream.Correlation;
import org.reaktivity.nukleus.socks.internal.stream.AbstractStreamProcessor;
import org.reaktivity.nukleus.socks.internal.stream.Context;
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
import org.reaktivity.nukleus.socks.internal.types.stream.TcpBeginExFW;
import org.reaktivity.nukleus.socks.internal.types.stream.WindowFW;

final class AcceptStreamProcessor extends AbstractStreamProcessor implements AcceptTransitionListener<TcpBeginExFW>
{
    private MessageConsumer streamState;
    private MessageConsumer acceptReplyThrottleState;
    private MessageConsumer connectThrottleState;

    private int slotIndex = NO_SLOT;
    private int slotOffset;

    private int acceptReplyWindowBytes = 0;
    private int acceptReplyWindowFrames = 0;

    private int connectWindowBytes = 0;
    private int connectWindowFrames = 0;

    private byte socksAtyp;
    private byte[] socksAddr;
    private int socksPort;

    final Correlation correlation;

    AcceptStreamProcessor(
        Context context,
        MessageConsumer acceptThrottle,
        long acceptStreamId,
        long acceptSourceRef,
        String acceptSourceName,
        long acceptCorrelationId)
    {
        super(context);
        final long acceptReplyStreamId = context.supplyStreamId.getAsLong();
        this.streamState = this::beforeBegin;
        this.acceptReplyThrottleState = this::handleAcceptReplyThrottleBeforeHandshake;
        this.connectThrottleState = this::handleConnectThrottleBeforeHandshake;
        context.router.setThrottle(
            acceptSourceName,
            acceptReplyStreamId,
            this::handleAcceptReplyThrottle);
        correlation = new Correlation();
        correlation.acceptThrottle(acceptThrottle);
        correlation.acceptStreamId(acceptStreamId);
        correlation.acceptSourceRef(acceptSourceRef);
        correlation.acceptSourceName(acceptSourceName);
        correlation.acceptCorrelationId(acceptCorrelationId);
        correlation.acceptReplyStreamId(acceptReplyStreamId);
        correlation.acceptReplyEndpoint(context.router.supplyTarget(acceptSourceName));
        correlation.acceptTransitionListener(this);
        correlation.connectStreamId(context.supplyStreamId.getAsLong());
        correlation.connectCorrelationId(context.supplyCorrelationId.getAsLong());
        correlation.acceptReplyEndpoint(context.router.supplyTarget(acceptSourceName));
        correlation.nextAcceptSignal(this::noop);
    }

    private RouteFW wrapRoute(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        return context.routeRO.wrap(buffer, index, index + length);
    }

    protected void handleStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        streamState.accept(msgTypeId, buffer, index, length);
    }

    protected void handleAcceptReplyThrottle(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        acceptReplyThrottleState.accept(msgTypeId, buffer, index, length);
    }

    protected void handleConnectThrottle(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        connectThrottleState.accept(msgTypeId, buffer, index, length);
    }

    @State
    private void beforeBegin(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        if (msgTypeId == BeginFW.TYPE_ID)
        {
            final BeginFW begin = context.beginRO.wrap(buffer, index, index + length);
            handleBegin(begin);
        }
        else
        {
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
        }
    }

    private void handleBegin(
        BeginFW begin)
    {
        BeginFW beginToAcceptReply = context.beginRW
            .wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
            .streamId(correlation.acceptReplyStreamId())
            .source("socks")
            .sourceRef(0) // Bi-directional reply
            .correlationId(correlation.acceptCorrelationId())
            .extension(e -> e.reset())
            .build();
        correlation.acceptReplyEndpoint().accept(
            beginToAcceptReply.typeId(),
            beginToAcceptReply.buffer(),
            beginToAcceptReply.offset(),
            beginToAcceptReply.sizeof());
        doWindow(
            correlation.acceptThrottle(),
            correlation.acceptStreamId(),
            65536,
            65536
        );
        this.streamState = this::afterBegin;
    }

    @State
    private void afterBegin(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            final DataFW data = context.dataRO.wrap(buffer, index, index + length);
            handleNegotiationData(data);
            break;
        case EndFW.TYPE_ID:
        case AbortFW.TYPE_ID:
            doAbort(correlation.acceptReplyEndpoint(), correlation.acceptReplyStreamId());
            break;
        default:
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
            break;
        }
    }

    private void handleNegotiationData(
        DataFW data)
    {
        OctetsFW payload = data.payload();
        DirectBuffer buffer = payload.buffer();
        int limit = payload.limit();
        int offset = payload.offset();
        int size = limit - offset;

        // Fragmented writes might have already occurred
        if (this.slotIndex != NO_SLOT)
        {
            MutableDirectBuffer acceptBuffer = context.bufferPool.buffer(this.slotIndex, this.slotOffset);
            acceptBuffer.putBytes(0, buffer, offset, size);
            this.slotOffset += size;                                  // New starting point is moved to the end of the buffer
            buffer = context.bufferPool.buffer(this.slotIndex); // Try to decode from the beginning of the buffer
            offset = 0;                                               //
            limit = this.slotOffset;                                  //
        }

        if (context.socksNegotiationRequestRO.canWrap(buffer, offset, limit)) // one negotiation request frame is in the buffer
        {
            final SocksNegotiationRequestFW socksNegotiation = context.socksNegotiationRequestRO.wrap(buffer, offset, limit);
            if (socksNegotiation.version() != 0x05)
            {
                throw new IllegalStateException(
                    String.format("Unsupported SOCKS protocol version (expected 0x05, received 0x%02x",
                        socksNegotiation.version()));
            }

            int nmethods = socksNegotiation.nmethods();
            byte i = 0;
            for (; i < nmethods; i++)
            {
                if (socksNegotiation.methods()[0] == (byte) 0x00)
                {
                    break;
                }
            }
            if (i == nmethods)
            {
                throw new IllegalStateException(
                    String.format("Unsupported SOCKS authentication method (expected 0x00, received 0x%02x",
                        socksNegotiation.methods()[0]));
            }
            correlation.nextAcceptSignal(this::attemptNegotiationResponse);
            correlation.nextAcceptSignal().accept(true);

            // Can safely release the buffer
            if (this.slotIndex != NO_SLOT)
            {
                context.bufferPool.release(this.slotIndex);
                this.slotOffset = 0;
                this.slotIndex = NO_SLOT;
            }
        }
        else if (this.slotIndex == NO_SLOT)
        {
            if (NO_SLOT == (slotIndex = context.bufferPool.acquire(correlation.acceptStreamId())))
            {
                doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
                return;
            }
            MutableDirectBuffer acceptBuffer = context.bufferPool.buffer(this.slotIndex);
            acceptBuffer.putBytes(0, buffer, offset, size);
            this.slotOffset = size;
        }
    }

    @Signal
    private void attemptNegotiationResponse(
        boolean isReadyState)
    {
        SocksNegotiationResponseFW socksNegotiationResponseFW = context.socksNegotiationResponseRW
            .wrap(context.writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, context.writeBuffer.capacity())
            .version((byte) 0x05)
            .method((byte) 0x00)
            .build();
        DataFW dataNegotiationResponseFW = context.dataRW.wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
            .streamId(correlation.acceptReplyStreamId())
            .payload(p -> p.set(
                socksNegotiationResponseFW.buffer(),
                socksNegotiationResponseFW.offset(),
                socksNegotiationResponseFW.sizeof()))
            .extension(e -> e.reset())
            .build();
        if (acceptReplyWindowBytes > dataNegotiationResponseFW.sizeof() && acceptReplyWindowFrames > 0)
        {
            this.streamState = this::afterNegotiation;
            correlation.nextAcceptSignal(this::noop);
            correlation.acceptReplyEndpoint().accept(
                dataNegotiationResponseFW.typeId(),
                dataNegotiationResponseFW.buffer(),
                dataNegotiationResponseFW.offset(),
                dataNegotiationResponseFW.sizeof());
            acceptReplyWindowBytes -= dataNegotiationResponseFW.sizeof();
            acceptReplyWindowFrames--;
        }
    }

    @Signal
    private void noop(
        boolean isReadyState)
    {
    }

    @State
    private void afterNegotiation(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            final DataFW data = context.dataRO.wrap(buffer, index, index + length);
            handleConnectRequestData(data);
            break;
        case EndFW.TYPE_ID:
        case AbortFW.TYPE_ID:
            doAbort(correlation.acceptReplyEndpoint(), correlation.acceptReplyStreamId());
            break;
        default:
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
            break;
        }
    }

    private void handleConnectRequestData(
        DataFW data)
    {
        OctetsFW payload = data.payload();
        DirectBuffer buffer = payload.buffer();
        int limit = payload.limit();
        int offset = payload.offset();
        int size = limit - offset;
        // Fragmented writes might have already occurred
        if (this.slotOffset != 0)
        {
            // Append incoming data to the buffer
            MutableDirectBuffer acceptBuffer = context.bufferPool.buffer(this.slotIndex, this.slotOffset);
            acceptBuffer.putBytes(0, buffer, offset, size);
            this.slotOffset += size;                                  // New starting point is moved to the end of the buffer
            buffer = context.bufferPool.buffer(this.slotIndex);       // Try to decode from the beginning of the buffer
            offset = 0;                                               //
            limit = this.slotOffset;                                  //
        }
        if (context.socksConnectionRequestRO.canWrap(buffer, offset, limit)) // one negotiation request frame is in the buffer
        {
            final SocksCommandRequestFW socksCommandRequestFW = context.socksConnectionRequestRO.wrap(buffer, offset, limit);
            final String dstAddrPort = socksCommandRequestFW.validateAndGetDstAddrPort();
            final RouteFW connectRoute = resolveTarget(
                correlation.acceptSourceRef(),
                correlation.acceptSourceName(),
                dstAddrPort
            );
            final String connectTargetName = connectRoute.target().asString();
            final MessageConsumer connectEndpoint = context.router.supplyTarget(connectTargetName);
            final long connectTargetRef = connectRoute.targetRef();
            final long connectStreamId = context.supplyStreamId.getAsLong();
            final long connectCorrelationId = context.supplyCorrelationId.getAsLong();
            correlation.connectRoute(connectRoute);
            correlation.connectEndpoint(connectEndpoint);
            correlation.connectTargetRef(connectTargetRef);
            correlation.connectTargetName(connectTargetName);
            correlation.connectStreamId(connectStreamId);
            correlation.connectCorrelationId(connectCorrelationId);
            context.correlations.put(connectCorrelationId, correlation); // Use this map on the CONNECT STREAM
            context.router.setThrottle(
                connectTargetName,
                connectStreamId,
                this::handleConnectThrottle
            );
            final BeginFW connectBegin = context.beginRW
                .wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
                .streamId(connectStreamId)
                .source("socks")
                .sourceRef(connectTargetRef)
                .correlationId(connectCorrelationId)
                .extension(e -> e.reset())
                .build();
            connectEndpoint.accept(
                connectBegin.typeId(),
                connectBegin.buffer(),
                connectBegin.offset(),
                connectBegin.sizeof()
            );
            correlation.nextAcceptSignal(this::noop);
            this.streamState = this::afterTargetConnectBegin;
            // Can safely release the buffer
            if (this.slotIndex != NO_SLOT)
            {
                context.bufferPool.release(this.slotIndex);
                this.slotOffset = 0;
                this.slotIndex = NO_SLOT;
            }
        }
        else if (this.slotIndex == NO_SLOT)
        {
            if (NO_SLOT == (slotIndex = context.bufferPool.acquire(correlation.acceptStreamId())))
            {
                doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
                return;
            }
            MutableDirectBuffer acceptBuffer = context.bufferPool.buffer(this.slotIndex);
            acceptBuffer.putBytes(0, buffer, offset, size);
            this.slotOffset = size;
        }
    }

    @State
    private void afterTargetConnectBegin(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
    }

    private void handleEnd(
        EndFW end)
    {
        // TODO IMPLEMENT ME
    }

    private void handleAbort(
        AbortFW abort)
    {
        // TODO IMPLEMENT ME
    }

    private void handleReset(
        ResetFW reset)
    {
        // TODO IMPLEMENT ME (or delete)
    }

    @Override
    public void transitionToConnectionReady(Optional<TcpBeginExFW> connectionInfo)
    {
        TcpBeginExFW tcpBeginEx = connectionInfo.get(); // Only reading so should be safe
        socksPort = tcpBeginEx.localPort();
        if (tcpBeginEx.localAddress().kind() == 1)
        {
            socksAtyp = (byte) 0x01;
            socksAddr = new byte[4];
        }
        else
        {
            socksAtyp = (byte) 0x04;
            socksAddr = new byte[16];
        }
        tcpBeginEx.localAddress()
            .ipv4Address()
            .get((directBuffer, offset, limit) ->
            {
                directBuffer.getBytes(offset, socksAddr);
                return null;
            });
        attemptConnectionResponse(false);
    }

    @Signal
    private void attemptConnectionResponse(
        boolean isReadyState)
    {
        if (!isReadyState)
        {
            correlation.nextAcceptSignal(this::attemptConnectionResponse);
        }
        SocksCommandResponseFW socksConnectResponseFW = context.socksConnectionResponseRW
            .wrap(context.writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, context.writeBuffer.capacity())
            .version((byte) 0x05)
            .reply((byte) 0x00) // CONNECT
            .bind(socksAtyp, socksAddr, socksPort)
            .build();
        DataFW dataConnectionResponseFW = context.dataRW.wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
            .streamId(correlation.acceptReplyStreamId())
            .payload(p -> p.set(
                socksConnectResponseFW.buffer(),
                socksConnectResponseFW.offset(),
                socksConnectResponseFW.sizeof()))
            .extension(e -> e.reset())
            .build();
        if (acceptReplyWindowBytes > dataConnectionResponseFW.sizeof() && acceptReplyWindowFrames > 0)
        {
            this.streamState = this::afterSourceConnect;
            this.acceptReplyThrottleState = this::handleAcceptReplyThrottleAfterHandshake;
            this.connectThrottleState = this::handleConnectThrottleAfterHandshake;

            correlation.acceptReplyEndpoint().accept(
                dataConnectionResponseFW.typeId(),
                dataConnectionResponseFW.buffer(),
                dataConnectionResponseFW.offset(),
                dataConnectionResponseFW.sizeof());

            doWindow(
                correlation.connectReplyThrottle(),
                correlation.connectReplyStreamId(),
                acceptReplyWindowBytes,
                acceptReplyWindowFrames);
            /*
            TODO THROTTLING
            Right now we have:
                - sent to accept throttle 65536 bytes and 65536 frames
                - received on accept ARB bytes and ARF frames
                - accumulated connectWindowBytes, connectWindowFrames from connect
            The current accept available window (CAW) is 65536 - ARB, 65536 - ARF
                if (AAW > connectWindowBytes) -> need to buffer until windows get aligned
                else just send to accept (connectWindowBytes - CAW)
            doWindow(
                correlation.acceptThrottle(),
                correlation.acceptStreamId(),
                connectWindowBytes,
                connectWindowFrames
            );
            */
        }
    }

    @State
    private void afterSourceConnect(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            final DataFW data = context.dataRO.wrap(buffer, index, index + length);
            handleHighLevelData(data);
            break;
        case EndFW.TYPE_ID:
        case AbortFW.TYPE_ID:
            doAbort(correlation.acceptReplyEndpoint(), correlation.acceptReplyStreamId());
            break;
        default:
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
            break;
        }
    }

    private void handleHighLevelData(DataFW data)
    {
        OctetsFW payload = data.payload();
        DataFW dataForwardFW = context.dataRW.wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
            .streamId(correlation.connectStreamId())
            .payload(p -> p.set(
                payload.buffer(),
                payload.offset(),
                payload.sizeof()))
            .extension(e -> e.reset())
            .build();
        correlation.connectEndpoint()
            .accept(
                dataForwardFW.typeId(),
                dataForwardFW.buffer(),
                dataForwardFW.offset(),
                dataForwardFW.sizeof());
    }

    private void handleAcceptReplyThrottleBeforeHandshake(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case WindowFW.TYPE_ID:
            final WindowFW window = context.windowRO.wrap(buffer, index, index + length);
            acceptReplyWindowBytes += window.update();
            acceptReplyWindowFrames += window.frames();
            correlation.nextAcceptSignal().accept(true);
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = context.resetRO.wrap(buffer, index, index + length);
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
            break;
        default:
            // ignore
            break;
        }
    }

    private void handleAcceptReplyThrottleAfterHandshake(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case WindowFW.TYPE_ID:
            final WindowFW window = context.windowRO.wrap(buffer, index, index + length);
            doWindow(
                correlation.connectReplyThrottle(),
                correlation.connectReplyStreamId(),
                window.update(),
                window.frames()
            );
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = context.resetRO.wrap(buffer, index, index + length);
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
            break;
        default:
            // ignore
            break;
        }
    }

    private void handleConnectThrottleBeforeHandshake(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case WindowFW.TYPE_ID:
            final WindowFW window = context.windowRO.wrap(buffer, index, index + length);
            connectWindowBytes += window.update();
            connectWindowFrames += window.frames();
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = context.resetRO.wrap(buffer, index, index + length);
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
            break;
        default:
            // ignore
            break;
        }
    }

    private void handleConnectThrottleAfterHandshake(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case WindowFW.TYPE_ID:
            final WindowFW window = context.windowRO.wrap(buffer, index, index + length);
            doWindow(
                correlation.acceptThrottle(),
                correlation.acceptStreamId(),
                window.update(),
                window.frames()
            );
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = context.resetRO.wrap(buffer, index, index + length);
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
            break;
        default:
            // ignore
            break;
        }
    }



    @Override
    public void transitionToAborted()
    {

    }

    RouteFW resolveTarget(
        long sourceRef,
        String sourceName,
        String dstAddrPort)
    {
        MessagePredicate filter = (t, b, o, l) ->
        {
            RouteFW route = context.routeRO.wrap(b, o, l);
            final SocksRouteExFW routeEx = route.extension()
                .get(context.routeExRO::wrap);
            return sourceRef == route.sourceRef() &&
                sourceName.equals(route.source().asString()) &&
                dstAddrPort.equals(routeEx.destAddrPort().asString());
        };
        return context.router.resolve(filter, this::wrapRoute);
    }
}
