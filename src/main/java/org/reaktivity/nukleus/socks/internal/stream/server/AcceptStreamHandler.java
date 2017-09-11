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
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
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
        final String acceptName = begin.source()
            .asString();
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
        int limit = data.limit();
        int offset = data.offset();
        int size = limit - offset;
        DirectBuffer buffer = data.buffer();

        /*
         * Fragmented writes might have already occurred, let's resume
         */
        if(this.slotLimit != 0)
        {
            /*
             * Get the previously created buffer
             *    1. The slotIndex was initialized in a previous wrapping attempt
             *    2. The slotOffset represents the offset at which writing will be done into the buffer
             *    3. What is slotLimit ?
             */
            MutableDirectBuffer acceptBuffer = streamContext.bufferPool.buffer(this.slotIndex, this.slotOffset);
            acceptBuffer.putBytes(this.slotLimit, buffer, offset, size);
            this.slotLimit += size;
            this.slotOffset = this.slotLimit;

            // re-align the input buffer
            buffer = acceptBuffer;
            offset = 0;
            limit = this.slotLimit;
            size =  this.slotLimit;
        }


        if(streamContext.socksNegotiationRO.canWrap(buffer, offset, limit))
        {

        }
        else
        {
            /*
             * Initialize the accumulation buffer
             */
            if (this.slotIndex == NO_SLOT)
            {
                this.slotIndex = streamContext.bufferPool.acquire(acceptReplyStreamId); // FIXME use the acceptStreamID
                MutableDirectBuffer acceptBuffer = streamContext.bufferPool.buffer(slotIndex);
                acceptBuffer.putBytes(0, buffer, offset, size);
                this.slotLimit = size;
            }
        }


        /*
         * Release the accumulation buffer
         */
        if (offset == limit && this.slotIndex != NO_SLOT)
        {
            streamContext.bufferPool.release(this.slotIndex);
            this.slotOffset = 0;
            this.slotIndex = NO_SLOT;
        }
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
