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

import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;

import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.socks.internal.metadata.State;
import org.reaktivity.nukleus.socks.internal.stream.AcceptTransitionListener;
import org.reaktivity.nukleus.socks.internal.stream.Context;
import org.reaktivity.nukleus.socks.internal.stream.Correlation;
import org.reaktivity.nukleus.socks.internal.stream.DefaultStreamHandler;
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

final class ConnectReplyStreamHandler extends DefaultStreamHandler
{
    private final MessageConsumer connectReplyThrottle;
    private final long connectReplyId;

    private MessageConsumer acceptReply;
    private MessageConsumer connect;

    private long acceptReplyId;
    private long connectId;

    private MessageConsumer streamState;

    private int targetWindowBytes;
    private int targetWindowFrames;

    private int targetWindowBytesAdjustment;
    private int targetWindowFramesAdjustment;

    private Consumer<WindowFW> windowHandler;

    private int slotIndex = NO_SLOT;
    private int slotOffset;

    private String destAddrPort;

    private String acceptReplyName;
    private long acceptCorrelationId;
    private AcceptTransitionListener acceptTransitionListener;


    ConnectReplyStreamHandler(
        Context context,
        MessageConsumer connectReplyThrottle,
        long connectReplyId)
    {
        super(context);
        this.connectReplyThrottle = connectReplyThrottle;
        this.connectReplyId = connectReplyId;
        this.streamState = this::beforeBegin;
        this.windowHandler = this::processInitialWindow; // TODO Should have more handlers depending on state ?
    }

    void handleStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        System.out.println("ServerConnectReplyStream doStream");
        streamState.accept(msgTypeId, buffer, index, length);
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
            doReset(connectReplyThrottle, connectReplyId);
        }
    }

    @State
    private void beforeNegotiationResponse(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            final DataFW data = context.dataRO.wrap(buffer, index, index + length);
            handleNegotiationResponse(data);
            break;
        case EndFW.TYPE_ID:
            final EndFW end = context.endRO.wrap(buffer, index, index + length);
            handleEnd(end);
            break;
        case AbortFW.TYPE_ID:
            final AbortFW abort = context.abortRO.wrap(buffer, index, index + length);
            handleAbort(abort);
            break;
        default:
            doReset(connectReplyThrottle, connectReplyId);
            break;
        }
    }

    private void handleBegin(BeginFW begin)
    {
        final long connectRef = begin.sourceRef();
        final long correlationId = begin.correlationId();
        final Correlation correlation = context.correlations.remove(correlationId);
        if (connectRef == 0L && correlation != null)
        {
            // Reply with Socks version 5 and "NO AUTHENTICATION REQUIRED"
            //            doWindow(acceptThrottle, acceptReplyStreamId, 1024, 1024); // TODO replace hardcoded values

            connect = context.router.supplyTarget(begin.source()
                .asString());
            connectId = correlation.connectStreamId();
            SocksNegotiationRequestFW socksNegotiationRequestFW = context.socksNegotiationRequestRW
                .wrap(context.writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, context.writeBuffer.capacity())
                .version((byte) 0x05)
                .nmethods((byte) 0x01)
                .method(new byte[]{0x00})
                .build();

            DataFW dataRequestFW = context.dataRW.wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
                .streamId(connectId)
                .payload(p -> p.set(
                    socksNegotiationRequestFW.buffer(),
                    socksNegotiationRequestFW.offset(),
                    socksNegotiationRequestFW.sizeof()))
                .extension(e -> e.reset())
                .build();

            connect.accept(
                dataRequestFW.typeId(),
                dataRequestFW.buffer(),
                dataRequestFW.offset(),
                dataRequestFW.sizeof());



            // TODO check if connectRoute can be passed through the correlation
            final RouteFW connectRoute = resolveSource(correlation.connectRef(), begin.source().asString());
            System.out.println(connectRoute);
            final SocksRouteExFW routeEx = connectRoute.extension().get(context.routeExRO::wrap);
            destAddrPort = routeEx.destAddrPort().asString();


            // TODO is this correct throttle consumer ?
            doWindow(this::handleThrottle, connectReplyId, 1024, 1024); // TODO replace hardcoded values

            // TODO only send this frame when Socks Connection has been established
            // TODO how to store the correlation object ? (just pass it along ?)
            // TODO Threading model - can we have a new stream that would overwrite data ?
            acceptReplyName = correlation.acceptName();
            acceptCorrelationId = correlation.acceptCorrelationId();
            acceptTransitionListener = correlation.acceptTransitionListener();
            this.streamState = this::beforeNegotiationResponse;
            this.windowHandler = this::processInitialWindow;
        }
        else
        {
            doReset(connectReplyThrottle, connectReplyId);
        }
    }

    // TODO might not be needed, could use Correlation
    RouteFW resolveSource(
        long targetRef,
        String targetName)
    {
        return context.router.resolve(
                (msgTypeId, buffer, offset, limit) ->
                {
                    RouteFW route = context.routeRO.wrap(buffer, offset, limit);
                    // TODO implement mode
                    return targetRef == route.targetRef() &&
                        targetName.equals(route.target()
                            .asString());
                },
                (msgTypeId, buffer, offset, length) ->
                {
                    return context.routeRO.wrap(buffer, offset, offset + length);
                }
            );
    }

    private void handleNegotiationResponse(DataFW data)
    {
        OctetsFW payload = data.payload();
        DirectBuffer buffer = payload.buffer();
        int limit = payload.limit();
        int offset = payload.offset();
        int size = limit - offset;

        // Fragmented writes might have already occurred
        if (this.slotIndex != NO_SLOT)
        {
            // Append incoming data to the buffer
            MutableDirectBuffer acceptBuffer = context.bufferPool.buffer(this.slotIndex, this.slotOffset);
            acceptBuffer.putBytes(0, buffer, offset, size);
            this.slotOffset += size;                                  // New starting point is moved to the end of the buffer
            buffer = context.bufferPool.buffer(this.slotIndex); // Try to decode from the beginning of the buffer
            offset = 0;                                               //
            limit = this.slotOffset;                                  //
        }

        if (context.socksNegotiationResponseRO.canWrap(buffer, offset, limit)) // one negotiation request frame is in the buffer
        {
            // Wrap the frame and extract the incoming data
            final SocksNegotiationResponseFW socksNegotiation = context.socksNegotiationResponseRO.wrap(buffer, offset, limit);
            if (socksNegotiation.version() != 0x05)
            {
                throw new IllegalStateException(
                    String.format("Unsupported SOCKS protocol version (expected 0x05, received 0x%02x",
                        socksNegotiation.version()));
            }

            // FIXME should allow multiple authentication methods if one of them is "No Authentication"
            if (socksNegotiation.method() != 0x00)
            {
                throw new IllegalStateException(
                    String.format("Unsupported SOCKS authentication methods (expected 0x00, received %02x",
                        socksNegotiation.method()));
            }

            // Reply with Socks version 5 and "NO AUTHENTICATION REQUIRED"
            // doWindow(acceptThrottle, acceptReplyStreamId, 1024, 1024); // TODO replace hardcoded values
            SocksCommandRequestFW socksConnectRequestFW = context.socksConnectionRequestRW
                .wrap(context.writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, context.writeBuffer.capacity())
                .version((byte) 0x05)
                .command((byte) 0x01) // CONNECT
                .destination(destAddrPort)
                .build();
            DataFW dataReplyFW = context.dataRW.wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
                .streamId(connectId)
                .payload(p -> p.set(
                    socksConnectRequestFW.buffer(),
                    socksConnectRequestFW.offset(),
                    socksConnectRequestFW.sizeof()))
                .extension(e -> e.reset())
                .build();
            connect.accept(
                dataReplyFW.typeId(),
                dataReplyFW.buffer(),
                dataReplyFW.offset(),
                dataReplyFW.sizeof());
            this.streamState = this::beforeConnectionResponse;

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
            // Initialize the accumulation buffer
            this.slotIndex = context.bufferPool.acquire(connectId);
            // FIXME might not get a slot, in this case should return an exception
            MutableDirectBuffer acceptBuffer = context.bufferPool.buffer(this.slotIndex);
            acceptBuffer.putBytes(0, buffer, offset, size);
            this.slotOffset = size;
        }
    }

    @State
    private void beforeConnectionResponse(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            final DataFW data = context.dataRO.wrap(buffer, index, index + length);
            handleConnectionResponse(data);
            break;
        case EndFW.TYPE_ID:
            final EndFW end = context.endRO.wrap(buffer, index, index + length);
            handleEnd(end);
            break;
        case AbortFW.TYPE_ID:
            final AbortFW abort = context.abortRO.wrap(buffer, index, index + length);
            handleAbort(abort);
            break;
        default:
            doReset(connectReplyThrottle, connectReplyId);
            break;
        }
    }


    private void handleConnectionResponse(DataFW data)
    {
        OctetsFW payload = data.payload();
        DirectBuffer buffer = payload.buffer();
        int limit = payload.limit();
        int offset = payload.offset();
        int size = limit - offset;

        // Fragmented writes might have already occurred
        if (this.slotIndex != NO_SLOT)
        {
            // Append incoming data to the buffer
            MutableDirectBuffer acceptBuffer = context.bufferPool.buffer(this.slotIndex, this.slotOffset);
            acceptBuffer.putBytes(0, buffer, offset, size);
            this.slotOffset += size;                                  // New starting point is moved to the end of the buffer
            buffer = context.bufferPool.buffer(this.slotIndex); // Try to decode from the beginning of the buffer
            offset = 0;                                               //
            limit = this.slotOffset;                                  //
        }

        if (context.socksConnectionResponseRO.canWrap(buffer, offset, limit)) // one negotiation request frame is in the buffer
        {
            // Wrap the frame and extract the incoming data
            final SocksCommandResponseFW socksConnectionResponse = context.socksConnectionResponseRO.wrap(buffer, offset, limit);
            if (socksConnectionResponse.version() != 0x05)
            {
                throw new IllegalStateException(
                    String.format("Unsupported SOCKS protocol version (expected 0x05, received 0x%02x",
                        socksConnectionResponse.version()));
            }

            if (socksConnectionResponse.reply() != 0x00)
            {
                throw new IllegalStateException(
                    String.format("Unsupported SOCKS connection reply (expected 0x00, received 0x%02x",
                        socksConnectionResponse.reply()));
            }

            final MessageConsumer newAcceptReply = context.router.supplyTarget(acceptReplyName);
            final long newAcceptReplyId = context.supplyStreamId.getAsLong();
            final long acceptReplyRef = 0; // Bi-directional reply
            BeginFW beginToAcceptReply = context.beginRW
                .wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
                .streamId(newAcceptReplyId)
                .source("socks")
                .sourceRef(acceptReplyRef)
                .correlationId(acceptCorrelationId)
                .extension(e -> e.reset())
                .build();
            newAcceptReply.accept(
                beginToAcceptReply.typeId(),
                beginToAcceptReply.buffer(),
                beginToAcceptReply.offset(),
                beginToAcceptReply.sizeof());

            // TODO understand why following 2 lines
            this.acceptReply = newAcceptReply;
            this.acceptReplyId = newAcceptReplyId;

            doWindow(connectReplyThrottle, connectReplyId, 1024, 1024); // TODO remove hardcoded values

            // Change current handler's state amd the one of the AcceptStreamHandler
            this.streamState = this::afterConnectionResponse;
            acceptTransitionListener.transitionToConnectionReady();

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
            // Initialize the accumulation buffer
            this.slotIndex = context.bufferPool.acquire(connectId);
            // FIXME might not get a slot, in this case should return an exception
            MutableDirectBuffer acceptBuffer = context.bufferPool.buffer(this.slotIndex);
            acceptBuffer.putBytes(0, buffer, offset, size);
            this.slotOffset = size;
        }
    }

    @State
    private void afterConnectionResponse(
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
            final EndFW end = context.endRO.wrap(buffer, index, index + length);
            handleEnd(end);
            break;
        case AbortFW.TYPE_ID:
            final AbortFW abort = context.abortRO.wrap(buffer, index, index + length);
            handleAbort(abort);
            break;
        default:
            doReset(connectReplyThrottle, connectReplyId);
            break;
        }
    }

    private void handleHighLevelData(DataFW data)
    {
        DataFW dataForwardFW = context.dataRW.wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
            .streamId(acceptReplyId)
            .payload(p -> p.set(
                data.payload().buffer(),
                data.payload().offset(),
                data.payload().sizeof()))
            .extension(e -> e.reset())
            .build();

        final MessageConsumer newAcceptReply = context.router.supplyTarget(acceptReplyName);
        this.acceptReply.accept(
            dataForwardFW.typeId(),
            dataForwardFW.buffer(),
            dataForwardFW.offset(),
            dataForwardFW.sizeof());

        // TODO understand why following line
//        this.acceptReply = newAcceptReply;

        doWindow(connectReplyThrottle, connectReplyId, 1024, 1024); // TODO remove hardcoded values
    }


    private void handleEnd(EndFW end)
    {
    }

    private void handleAbort(AbortFW abort)
    {
    }

    private void handleThrottle(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case WindowFW.TYPE_ID:
            final WindowFW window = context.windowRO.wrap(buffer, index, index + length);
            this.windowHandler.accept(window);
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = context.resetRO.wrap(buffer, index, index + length);
            handleReset(reset);
            break;
        default:
            // ignore
            break;
        }
    }

    private void processInitialWindow(WindowFW window)
    {
        final int sourceWindowBytesDelta = window.update();

        targetWindowBytesAdjustment -= sourceWindowBytesDelta * 20 / 100;

        this.windowHandler = this::processWindow;
        this.windowHandler.accept(window);
    }

    private void processWindow(WindowFW window)
    {
        final int sourceWindowBytesDelta = window.update();
        final int sourceWindowFramesDelta = window.frames();

        final int targetWindowBytesDelta = sourceWindowBytesDelta + targetWindowBytesAdjustment;
        final int targetWindowFramesDelta = sourceWindowFramesDelta + targetWindowFramesAdjustment;

        targetWindowBytes += Math.max(targetWindowBytesDelta, 0);
        targetWindowBytesAdjustment = Math.min(targetWindowBytesDelta, 0);

        targetWindowFrames += Math.max(targetWindowFramesDelta, 0);
        targetWindowFramesAdjustment = Math.min(targetWindowFramesDelta, 0);

        if (targetWindowBytesDelta > 0 || targetWindowFramesDelta > 0)
        {
            doWindow(connectReplyThrottle, connectReplyId, Math.max(targetWindowBytesDelta, 0),
                Math.max(targetWindowFramesDelta, 0));
        }
    }

    private void handleReset(ResetFW reset)
    {
        doReset(connectReplyThrottle, connectReplyId);
    }
}
