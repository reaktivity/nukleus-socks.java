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

import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.socks.internal.stream.types.FragmentedFlyweight;
import org.reaktivity.nukleus.socks.internal.stream.types.FragmentedHandler;
import org.reaktivity.nukleus.socks.internal.stream.types.FragmentedSenderComplete;
import org.reaktivity.nukleus.socks.internal.stream.types.FragmentedSenderPartial;
import org.reaktivity.nukleus.socks.internal.types.Flyweight;
import org.reaktivity.nukleus.socks.internal.types.OctetsFW;
import org.reaktivity.nukleus.socks.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.socks.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.socks.internal.types.stream.DataFW;
import org.reaktivity.nukleus.socks.internal.types.stream.EndFW;
import org.reaktivity.nukleus.socks.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.socks.internal.types.stream.WindowFW;

public abstract class AbstractStreamProcessor
{
    public static final String NUKLEUS_SOCKS_NAME = "socks";

    protected final Context context;

    protected int slotIndex = NO_SLOT;
    protected int slotReadOffset;
    protected int slotWriteOffset;

    protected int attemptOffset;

    public AbstractStreamProcessor(Context context)
    {
        this.context = context;
    }

    protected abstract void handleStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length);

    protected void doBegin(
        MessageConsumer streamConsumer,
        long streamId,
        long sourceRef,
        long correlationId)
    {
        BeginFW beginToAcceptReply = context.beginRW
            .wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
            .streamId(streamId)
            .source("socks")
            .sourceRef(sourceRef)
            .correlationId(correlationId)
            .extension(e -> e.reset())
            .build();
        streamConsumer.accept(
            beginToAcceptReply.typeId(),
            beginToAcceptReply.buffer(),
            beginToAcceptReply.offset(),
            beginToAcceptReply.sizeof());
    }

    protected void doAbort(
        MessageConsumer stream,
        long streamId)
    {
        final AbortFW abort =
            context.abortRW
                .wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
                .streamId(streamId)
                .build();
        // System.out.println("Sending abort: " + abort+ " to stream: " + stream);
        stream.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    protected void doEnd(
        MessageConsumer stream,
        long streamId)
    {
        final EndFW end =
            context.endRW
                .wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
                .streamId(streamId)
                .build();
        // System.out.println("Sending end: " + end + " to stream: " + stream);
        stream.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    protected void doWindow(
        final MessageConsumer throttle,
        final long throttleId,
        final int credit,
        final int padding)
    {
        final WindowFW window = context.windowRW
            .wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
            .streamId(throttleId)
            .credit(credit)
            .padding(padding)
            .build();
        System.out.println("\tSending window: " + window);
        throttle.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    protected void doReset(
        final MessageConsumer throttle,
        final long throttleId)
    {
        final ResetFW reset =
            context.resetRW
                .wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
                .streamId(throttleId)
                .build();
        throttle.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    protected void handleFragmentedData(
        DataFW from,
        FragmentedFlyweight to,
        FragmentedHandler handler,
        MessageConsumer throttle,
        long throttleStreamId)
    {
        OctetsFW payload = from.payload();
        DirectBuffer buffer = payload.buffer();
        int limit = payload.limit();
        int offset = payload.offset();
        int size = limit - offset;
        // Fragmented writes might have already occurred
        if (slotIndex != NO_SLOT)
        {
            MutableDirectBuffer acceptBuffer = context.bufferPool.buffer(slotIndex, slotWriteOffset);
            acceptBuffer.putBytes(0, buffer, offset, size);
            slotWriteOffset += size;                                  // New starting point is moved to the end of the buffer
            buffer = context.bufferPool.buffer(slotIndex);            // Try to decode from the beginning of the buffer
            offset = 0;                                                    //
            limit = slotWriteOffset;                                  //
        }
        FragmentedFlyweight.ReadState fragmentationState = to.canWrap(buffer, offset, limit);
        if (fragmentationState == FragmentedFlyweight.ReadState.FULL) // one negotiation request frame is in the buffer
        {
            handler.handle(to, buffer, offset, limit);
        }
        else if (fragmentationState == FragmentedFlyweight.ReadState.BROKEN)
        {
            doReset(throttle, throttleStreamId);
        }
        else if (slotIndex == NO_SLOT)
        {
            assert fragmentationState == FragmentedFlyweight.ReadState.INCOMPLETE;
            if (NO_SLOT == (slotIndex = context.bufferPool.acquire(throttleStreamId)))
            {
                doReset(throttle, throttleStreamId);
                return;
            }
            MutableDirectBuffer acceptBuffer = context.bufferPool.buffer(slotIndex);
            acceptBuffer.putBytes(0, buffer, offset, size);
            slotWriteOffset = size;
        }
        if (fragmentationState != FragmentedFlyweight.ReadState.INCOMPLETE &&
            slotIndex != NO_SLOT)
        {
            // Can safely release the buffer
            context.bufferPool.release(slotIndex);
            slotWriteOffset = 0;
            slotIndex = NO_SLOT;
        }
    }

    protected void doFragmentedData(
        Flyweight flyweight,
        final int targetCredit,
        final int targetPadding,
        MessageConsumer target,
        long targetStreamId,
        FragmentedSenderPartial senderPartial,
        FragmentedSenderComplete senderComplete)
    {
        System.out.println("\t>>>>doFragmentedData");
        System.out.println("\t\ttargetCredit: " + targetCredit);
        System.out.println("\t\ttargetPadding: " + targetPadding);
        System.out.println("\t\tflyweight.sizeof: " + flyweight.sizeof());
        System.out.println("\t\tattemptOffset: " + attemptOffset);
        final int targetBytes = targetCredit - targetPadding;
        if (targetBytes >= flyweight.sizeof() - attemptOffset)
        {
            DataFW dataFW = context.dataRW
                .wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
                .streamId(targetStreamId)
                .payload(p -> p.set(
                    flyweight.buffer(),
                    flyweight.offset() + attemptOffset,
                    flyweight.sizeof() - attemptOffset))
                .build();
            target.accept(
                dataFW.typeId(),
                dataFW.buffer(),
                dataFW.offset(),
                dataFW.sizeof());
            senderComplete.update(flyweight.sizeof() - attemptOffset + targetPadding);
            attemptOffset = 0;
        }
        else if (targetBytes > 0)
        {
            System.out.println("\tSending partial flyweight");
            System.out.println("\tattemptOffset: " + attemptOffset);
            System.out.println("\ttargetCredit: " + targetCredit);
            DataFW dataFW = context.dataRW
                .wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
                .streamId(targetStreamId)
                .payload(p -> p.set(
                    flyweight.buffer(),
                    flyweight.offset() + attemptOffset,
                    targetBytes))
                .build();
            target.accept(
                dataFW.typeId(),
                dataFW.buffer(),
                dataFW.offset(),
                dataFW.sizeof());
            senderPartial.update(targetCredit);
            attemptOffset += targetBytes;
        }
    }

    protected void doForwardData(
        OctetsFW payload,
        long streamId,
        MessageConsumer streamEndpoint)
    {
        doForwardData(
            payload,
            payload.sizeof(),
            streamId,
            streamEndpoint);
    }

    protected void doForwardData(
        OctetsFW payload,
        int payloadLength,
        long streamId,
        MessageConsumer streamEndpoint)
    {
        doForwardData(
            payload.buffer(),
            payload.offset(),
            payloadLength,
            streamId,
            streamEndpoint);
    }

    protected void doForwardData(
        DirectBuffer payloadBuffer,
        int payloadOffset,
        int payloadLength,
        long streamId,
        MessageConsumer streamEndpoint)
    {
        DataFW dataForwardFW = context.dataRW.wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
            .streamId(streamId)
            .payload(p -> p.set(
                payloadBuffer,
                payloadOffset,
                payloadLength))
            .extension(e -> e.reset())
            .build();
        System.out.println("Forwarding data frame: ");
        System.out.println("\t" + dataForwardFW);
        streamEndpoint.accept(
            dataForwardFW.typeId(),
            dataForwardFW.buffer(),
            dataForwardFW.offset(),
            dataForwardFW.sizeof());
    }
}
