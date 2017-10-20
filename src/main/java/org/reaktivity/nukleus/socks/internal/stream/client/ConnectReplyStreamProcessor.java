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

import java.util.Optional;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.socks.internal.metadata.State;
import org.reaktivity.nukleus.socks.internal.stream.AbstractStreamProcessor;
import org.reaktivity.nukleus.socks.internal.stream.Context;
import org.reaktivity.nukleus.socks.internal.stream.Correlation;
import org.reaktivity.nukleus.socks.internal.stream.types.Fragmented;
import org.reaktivity.nukleus.socks.internal.stream.types.SocksCommandResponseFW;
import org.reaktivity.nukleus.socks.internal.stream.types.SocksNegotiationResponseFW;
import org.reaktivity.nukleus.socks.internal.types.OctetsFW;
import org.reaktivity.nukleus.socks.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.socks.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.socks.internal.types.stream.DataFW;
import org.reaktivity.nukleus.socks.internal.types.stream.EndFW;
import org.reaktivity.nukleus.socks.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.socks.internal.types.stream.WindowFW;

final class ConnectReplyStreamProcessor extends AbstractStreamProcessor
{

    public static final int MAX_WRITABLE_BYTES = 65535;
    public static final int MAX_WRITABLE_FRAMES = 65535;

    private MessageConsumer streamState;

    private int acceptReplyWindowBytesAdjustment;
    private int acceptReplyWindowFramesAdjustment;

    private int slotIndex = NO_SLOT;
    private int slotOffset;
    Correlation correlation;
    private final MessageConsumer connectReplyThrottle;
    private final long connectReplyStreamId;

    ConnectReplyStreamProcessor(
        Context context,
        MessageConsumer connectReplyThrottle,
        long connectReplyId)
    {
        super(context);
        this.streamState = this::beforeBegin;
        this.connectReplyThrottle = connectReplyThrottle;
        this.connectReplyStreamId = connectReplyId;
    }

    @Override
    protected void handleStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
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
            doReset(connectReplyThrottle, connectReplyStreamId);
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
            doReset(connectReplyThrottle, connectReplyStreamId);
            break;
        }
    }

    private void handleBegin(BeginFW begin)
    {
        final long connectRef = begin.sourceRef();
        final long correlationId = begin.correlationId();
        correlation = context.correlations.remove(correlationId);
        if (connectRef == 0L && correlation != null)
        {
//            context.router.setThrottle(
//                correlation.acceptSourceName(),
//                correlation.acceptReplyStreamId(),
//                this::handleAcceptReplyThrottle);

            correlation.connectReplyThrottle(connectReplyThrottle);
            correlation.connectReplyStreamId(connectReplyStreamId);

            final long acceptReplyRef = 0; // Bi-directional reply
            BeginFW beginToAcceptReply = context.beginRW
                .wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
                .streamId(correlation.acceptReplyStreamId())
                .source("socks")
                .sourceRef(acceptReplyRef)
                .correlationId(correlation.acceptCorrelationId())
                .extension(e -> e.reset())
                .build();
            correlation.acceptReplyEndpoint()
                .accept(
                    beginToAcceptReply.typeId(),
                    beginToAcceptReply.buffer(),
                    beginToAcceptReply.offset(),
                    beginToAcceptReply.sizeof());

            streamState = this::beforeNegotiationResponse;
            System.out.println("CONNECT-REPLY/handleBegin");
            doWindow(
                connectReplyThrottle,
                connectReplyStreamId,
                MAX_WRITABLE_BYTES,
                MAX_WRITABLE_FRAMES
            );
        }
        else
        {
            doReset(connectReplyThrottle, connectReplyStreamId);
        }
    }

    private void handleNegotiationResponse(DataFW data)
    {
        OctetsFW payload = data.payload();
        DirectBuffer buffer = payload.buffer();
        int limit = payload.limit();
        int offset = payload.offset();
        int size = limit - offset;
        // Fragmented writes might have already occurred
        if (slotIndex != NO_SLOT)
        {
            // Append incoming data to the buffer
            MutableDirectBuffer acceptBuffer = context.bufferPool.buffer(slotIndex, slotOffset);
            acceptBuffer.putBytes(0, buffer, offset, size);
            slotOffset += size;                                  // Next starting point is moved to the end of the buffer
            buffer = context.bufferPool.buffer(slotIndex);       // Try to decode from the beginning of the buffer
            offset = 0;                                          //
            limit = slotOffset;                                  //
        }
        Fragmented.ReadState fragmentationState = context.socksNegotiationResponseRO.canWrap(buffer, offset, limit);
        // one negotiation request frame is in the buffer
        if (fragmentationState == Fragmented.ReadState.FULL)
        {
            // Wrap the frame and extract the incoming data
            final SocksNegotiationResponseFW socksNegotiationResponse =
                context.socksNegotiationResponseRO.wrap(buffer, offset, limit);
            if (socksNegotiationResponse.version() != 0x05 ||
                socksNegotiationResponse.method() != 0x00)
            {
                // TODO diagnostic
                doReset(connectReplyThrottle, connectReplyStreamId);
            }
            streamState = this::beforeConnectionResponse;
            correlation.nextAcceptSignal().accept(true);
        }
        else if (fragmentationState == Fragmented.ReadState.BROKEN)
        {
            doReset(connectReplyThrottle, connectReplyStreamId);
        }
        else if (slotIndex == NO_SLOT)
        {
            assert fragmentationState == Fragmented.ReadState.INCOMPLETE;
            if (NO_SLOT == (slotIndex = context.bufferPool.acquire(correlation.connectStreamId())))
            {
                doReset(connectReplyThrottle, connectReplyStreamId);
                return;
            }
            MutableDirectBuffer acceptBuffer = context.bufferPool.buffer(slotIndex);
            acceptBuffer.putBytes(0, buffer, offset, size);
            slotOffset = size;
        }
        if (fragmentationState != Fragmented.ReadState.INCOMPLETE &&
            this.slotIndex != NO_SLOT)
        {
            // Can safely release the buffer
            context.bufferPool.release(this.slotIndex);
            this.slotOffset = 0;
            this.slotIndex = NO_SLOT;
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
            doReset(connectReplyThrottle, connectReplyStreamId);
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
        if (slotIndex != NO_SLOT)
        {
            // Append incoming data to the buffer
            MutableDirectBuffer acceptBuffer = context.bufferPool.buffer(slotIndex, slotOffset);
            acceptBuffer.putBytes(0, buffer, offset, size);
            slotOffset += size;                                  // Next starting point is moved to the end of the buffer
            buffer = context.bufferPool.buffer(slotIndex);       // Try to decode from the beginning of the buffer
            offset = 0;                                          //
            limit = slotOffset;                                  //
        }
        Fragmented.ReadState fragmentationState = context.socksConnectionResponseRO.canWrap(buffer, offset, limit);
        // one negotiation request frame is in the buffer
        if (fragmentationState == Fragmented.ReadState.FULL)
        {
            // Wrap the frame and extract the incoming data
            final SocksCommandResponseFW socksConnectionResponse = context.socksConnectionResponseRO.wrap(buffer, offset, limit);
            if (socksConnectionResponse.version() != 0x05 ||
                socksConnectionResponse.reply() != 0x00)
            {
                // TODO diagnostic
                doReset(connectReplyThrottle, connectReplyStreamId);
            }
            // State machine transitions
            streamState = this::afterConnectionResponse;
            correlation.acceptTransitionListener().transitionToConnectionReady(Optional.empty());
        }
        else if (fragmentationState == Fragmented.ReadState.BROKEN)
        {
            doReset(connectReplyThrottle, connectReplyStreamId);
        }
        else if (slotIndex == NO_SLOT)
        {
            assert fragmentationState == Fragmented.ReadState.INCOMPLETE;
            if (NO_SLOT == (slotIndex = context.bufferPool.acquire(correlation.connectStreamId())))
            {
                doReset(connectReplyThrottle, connectReplyStreamId);
                return;
            }
            MutableDirectBuffer acceptBuffer = context.bufferPool.buffer(slotIndex);
            acceptBuffer.putBytes(0, buffer, offset, size);
            slotOffset = size;
        }
        if (fragmentationState != Fragmented.ReadState.INCOMPLETE &&
            this.slotIndex != NO_SLOT)
        {
            // Can safely release the buffer
            context.bufferPool.release(this.slotIndex);
            this.slotOffset = 0;
            this.slotIndex = NO_SLOT;
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
            doReset(connectReplyThrottle, connectReplyStreamId);
            break;
        }
    }

    private void handleHighLevelData(DataFW data)
    {
        System.out.println("CONNECT-REPLY/handleHighLevelData");
        OctetsFW payload = data.payload();
        DataFW dataForwardFW = context.dataRW.wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
            .streamId(correlation.acceptReplyStreamId())
            .payload(p -> p.set(
                payload.buffer(),
                payload.offset(),
                payload.sizeof()))
            .extension(e -> e.reset())
            .build();
        System.out.println("\t forwarding: " + data);
        System.out.println("\t to: " + dataForwardFW);
        correlation.acceptReplyEndpoint()
            .accept(
                dataForwardFW.typeId(),
                dataForwardFW.buffer(),
                dataForwardFW.offset(),
                dataForwardFW.sizeof());
    }

    private void handleEnd(EndFW end)
    {
        EndFW endForwardFW = context.endRW
            .wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
            .streamId(correlation.acceptReplyStreamId())
            .build();
        correlation.acceptReplyEndpoint()
            .accept(
                endForwardFW.typeId(),
                endForwardFW.buffer(),
                endForwardFW.offset(),
                endForwardFW.sizeof());
    }

    private void handleAbort(AbortFW abort)
    {
        doAbort(correlation.acceptReplyEndpoint(), correlation.acceptReplyStreamId());
        correlation.acceptTransitionListener().transitionToAborted();
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
            final WindowFW window = context.windowRO.wrap(buffer, index, index + length);
            System.out.println("CONNECT-REPLY/processWindow");
            doWindow(
                connectReplyThrottle,
                connectReplyStreamId,
                window.update(),
                window.frames()
            );
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

    private void handleReset(ResetFW reset)
    {
        doReset(connectReplyThrottle, connectReplyStreamId);
    }
}
