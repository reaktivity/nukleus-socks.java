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

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.socks.internal.stream.protocol.SocksNegotiationRequestFW;
import org.reaktivity.nukleus.socks.internal.stream.protocol.SocksNegotiationResponseFW;
import org.reaktivity.nukleus.socks.internal.types.OctetsFW;
import org.reaktivity.nukleus.socks.internal.types.control.RouteFW;
import org.reaktivity.nukleus.socks.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.socks.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.socks.internal.types.stream.DataFW;
import org.reaktivity.nukleus.socks.internal.types.stream.EndFW;
import org.reaktivity.nukleus.socks.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.socks.internal.types.stream.WindowFW;


final class AcceptStreamHandler extends DefaultStreamHandler
{
    private final MessageConsumer acceptThrottle;
    private final long acceptId;

    private MessageConsumer connectTarget;
    private long connectId;

    private MessageConsumer streamState;

    private int slotIndex = NO_SLOT;
    private int slotLimit;
    private int slotOffset;

    private int payloadProgress;
    private int payloadLength;
    private int maskingKey;

    private int acceptWindowBytes;
    private int acceptWindowFrames;
    private int sourceWindowBytesAdjustment;
    private int sourceWindowFramesAdjustment;


    MessageConsumer acceptReply;
    long acceptReplyStreamId;
    String acceptName;


    AcceptStreamHandler(
        StreamContext streamContext,
        MessageConsumer acceptThrottle,
        long acceptId,
        long acceptRef)
    {
        super(streamContext);
        this.acceptThrottle = acceptThrottle;
        this.acceptId = acceptId;
        this.streamState = this::beforeBegin;
    }

    RouteFW resolveTarget(
        long sourceRef,
        String sourceName)
    {
        MessagePredicate filter = (t, b, o, l) ->
        {
            RouteFW route = streamContext.routeRO.wrap(b, o, l);
            return sourceRef == route.sourceRef() &&
                sourceName.equals(route.source().asString());
        };
        return streamContext.router.resolve(filter, this::wrapRoute);
    }

    private RouteFW wrapRoute(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        return streamContext.routeRO.wrap(buffer, index, index + length);
    }

    void handleStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        streamState.accept(msgTypeId, buffer, index, length);
    }

    private void beforeBegin(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        if (msgTypeId == BeginFW.TYPE_ID)
        {
            final BeginFW begin = streamContext.beginRO.wrap(buffer, index, index + length);
            handleBegin(begin);
        }
        else
        {
            doReset(acceptThrottle, acceptId);
        }
    }

    private void handleBegin(
        BeginFW begin)
    {
        System.out.println("Handle Begin");
        // ACCEPT STREAM
        this.acceptName = begin.source().asString();
        final long acceptRef = begin.sourceRef();
        final long acceptCorrelationId = begin.correlationId();
        final long acceptStreamId = begin.streamId();

        ///////////////  ACCEPT REPLY STREAM /////////////////////////////
        // acceptCorrelationId = acceptCorrelationId; They are the same for acceptReply
        // and accept stream
        { // Readability indent, will remove
            this.acceptReply = streamContext.router.supplyTarget(acceptName);
            this.acceptReplyStreamId = streamContext.supplyStreamId.getAsLong();
            final long acceptReplyRef = 0; // Bi-directional reply

            // to WRITE to AcceptReplyStream
            BeginFW beginToAcceptReply = streamContext.beginRW
                .wrap(streamContext.writeBuffer, 0, streamContext.writeBuffer.capacity())
                .streamId(acceptReplyStreamId)
                .source("socks")
                .sourceRef(acceptReplyRef)
                .correlationId(acceptCorrelationId)
                .extension(e -> e.reset()) // TODO SOCKS5 handshake
                .build();

            acceptReply.accept(
                beginToAcceptReply.typeId(),
                beginToAcceptReply.buffer(),
                beginToAcceptReply.offset(),
                beginToAcceptReply.sizeof());

            streamContext.router.setThrottle(acceptName, acceptReplyStreamId, this::handleAcceptReplyThrottle);
        }

/*        /////////////// CONNECT STREAM //////////////////
        { // Readability indent, will remove
            final RouteFW connectRoute = resolveTarget(acceptRef, acceptName);
            final String connectName = connectRoute.target()
                .asString();
            final MessageConsumer connect = streamContext.router.supplyTarget(connectName);
            final long connectRef = connectRoute.targetRef();

            // Initialize connect stream
            final long connectStreamId = streamContext.supplyStreamId.getAsLong();
            final long connectCorrelationId = streamContext.supplyCorrelationId.getAsLong();
            streamContext.correlations.put(connectCorrelationId, new Correlation()); // Use this map on the CONNECT STREAM

            // TODO: need lightweight approach (start)
            final BeginFW connectBegin = streamContext.beginRW
                .wrap(streamContext.writeBuffer, 0, streamContext.writeBuffer.capacity())
                .streamId(connectStreamId)
                .source("socks")
                .sourceRef(connectRef)
                .correlationId(connectCorrelationId)
                // .extension(e -> e.reset())
                .build();
            connect.accept(connectBegin.typeId(), connectBegin.buffer(), connectBegin.offset(), connectBegin.sizeof());
            streamContext.router.setThrottle(connectName, connectStreamId, this::handleAcceptReplyThrottle);
        }*/

        // tell accept stream you can handle more data
        doWindow(acceptThrottle, begin.streamId(), 1024, 1024); // TODO replace hardcoded values
        this.streamState = this::afterBegin;
    }

    private void afterBegin(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            final DataFW data = streamContext.dataRO.wrap(buffer, index, index + length);
            handleNegotiationData(data);
            break;
        case EndFW.TYPE_ID:
        case AbortFW.TYPE_ID:
            doAbort(acceptReply, acceptReplyStreamId);
            break;
        default:
            doReset(acceptThrottle, acceptId);
            break;
        }
    }


    private void handleAcceptReplyThrottle(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case WindowFW.TYPE_ID:
            final WindowFW window = streamContext.windowRO.wrap(buffer, index, index + length);
            handleWindow(window);
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = streamContext.resetRO.wrap(buffer, index, index + length);
            handleReset(reset);
            break;
        default:
            // ignore
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
        if(this.slotOffset != 0)
        {
            // Append incoming data to the buffer
            MutableDirectBuffer acceptBuffer = streamContext.bufferPool.buffer(this.slotIndex, this.slotOffset);
            acceptBuffer.putBytes(0, buffer, offset, size);
            this.slotOffset += size;                                  // New starting point is moved to the end of the buffer
            buffer = streamContext.bufferPool.buffer(this.slotIndex); // Try to decode from the beginning of the buffer
            offset = 0;                                               //
            limit = this.slotOffset;                                  //
        }

        if(streamContext.socksNegotiationRO.canWrap(buffer, offset, limit)) // one negotiation request frame is in the buffer
        {
            if (this.slotIndex != NO_SLOT) // Can safely release the buffer
            {
                streamContext.bufferPool.release(this.slotIndex);
                this.slotOffset = 0;
                this.slotIndex = NO_SLOT;
            }


            // Wrap the frame and extract the incoming data
            final SocksNegotiationRequestFW socksNegotiation = streamContext.socksNegotiationRO.wrap(buffer, offset, limit);
            if (socksNegotiation.version() != 0x05)
            {
                throw new IllegalStateException(
                    String.format("Unsupported SOCKS protocol version (expected 0x05, received 0x%02x",
                        socksNegotiation.version()));
            }

            // FIXME should allow multiple authentication methods if one of them is "No Authentication"
            if (socksNegotiation.nmethods() != 0x01)
            {
                throw new IllegalStateException(
                    String.format("Unsupported SOCKS number of authentication methods (expected 1, received %d",
                        socksNegotiation.nmethods()));
            }

            if (socksNegotiation.methods()[0] != (byte) 0x00)
            {
                throw new IllegalStateException(
                    String.format("Unsupported SOCKS authentication method (expected 0x00, received 0x%02x",
                        socksNegotiation.methods()[0]));
            }

            // Reply with Socks version 5 and "NO AUTHENTICATION REQUIRED"
            doWindow(acceptThrottle, acceptReplyStreamId, 1024, 1024); // TODO replace hardcoded values
            SocksNegotiationResponseFW socksNegotiationResponseFW = streamContext.socksNegotiationRW
                .wrap(streamContext.writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, streamContext.writeBuffer.capacity())
                .version((byte) 0x05)
                .method((byte) 0x00)
                .build();
            DataFW dataReplyFW = streamContext.dataRW.wrap(streamContext.writeBuffer, 0, streamContext.writeBuffer.capacity())
                .streamId(acceptReplyStreamId)
                .payload(p -> p.set(socksNegotiationResponseFW.buffer(), socksNegotiationResponseFW.offset(), socksNegotiationResponseFW.sizeof()))
                .extension(e -> e.reset())
                .build();
            acceptReply.accept(
                dataReplyFW.typeId(),
                dataReplyFW.buffer(),
                dataReplyFW.offset(),
                dataReplyFW.sizeof());

            this.streamState = this::afterNegotiation;
        }
        else if (this.slotIndex == NO_SLOT)
        {
            // Initialize the accumulation buffer
            this.slotIndex = streamContext.bufferPool.acquire(acceptReplyStreamId);
            // FIXME use the acceptStreamID
            // FIXME might not get a slot, in this case should return an exception
            MutableDirectBuffer acceptBuffer = streamContext.bufferPool.buffer(this.slotIndex);
            acceptBuffer.putBytes(0, buffer, offset, size);
            this.slotOffset = size;
        }
    }

    private void afterNegotiation(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            final DataFW data = streamContext.dataRO.wrap(buffer, index, index + length);
            handleConnectRequestData(data);
            break;
        case EndFW.TYPE_ID:
        case AbortFW.TYPE_ID:
            doAbort(acceptReply, acceptReplyStreamId);
            break;
        default:
            doReset(acceptThrottle, acceptId);
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

    }


    private void handleEnd(
        EndFW end)
    {
    }

    private void handleAbort(
        AbortFW abort)
    {
        // TODO: WsAbortEx
    }

    private void handleWindow(
        WindowFW window)
    {
        final int targetWindowBytesDelta = window.update();
        final int targetWindowFramesDelta = window.frames();

        final int sourceWindowBytesDelta = targetWindowBytesDelta + sourceWindowBytesAdjustment;
        final int sourceWindowFramesDelta = targetWindowFramesDelta + sourceWindowFramesAdjustment;

        acceptWindowBytes += Math.max(sourceWindowBytesDelta, 0);
        sourceWindowBytesAdjustment = Math.min(sourceWindowBytesDelta, 0);

        acceptWindowFrames += Math.max(sourceWindowFramesDelta, 0);
        sourceWindowFramesAdjustment = Math.min(sourceWindowFramesDelta, 0);

        if (sourceWindowBytesDelta > 0 || sourceWindowFramesDelta > 0)
        {
            doWindow(acceptThrottle, acceptId, Math.max(sourceWindowBytesDelta, 0), Math.max(sourceWindowFramesDelta, 0));
        }
    }

    private void handleReset(
        ResetFW reset)
    {
        doReset(acceptThrottle, acceptId);
    }
}
