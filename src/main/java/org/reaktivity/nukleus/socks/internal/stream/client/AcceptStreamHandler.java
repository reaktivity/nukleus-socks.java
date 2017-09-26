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

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.socks.internal.metadata.State;
import org.reaktivity.nukleus.socks.internal.stream.AcceptTransitionListener;
import org.reaktivity.nukleus.socks.internal.stream.Context;
import org.reaktivity.nukleus.socks.internal.stream.Correlation;
import org.reaktivity.nukleus.socks.internal.stream.DefaultStreamHandler;
import org.reaktivity.nukleus.socks.internal.types.control.RouteFW;
import org.reaktivity.nukleus.socks.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.socks.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.socks.internal.types.stream.DataFW;
import org.reaktivity.nukleus.socks.internal.types.stream.EndFW;
import org.reaktivity.nukleus.socks.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.socks.internal.types.stream.WindowFW;

public final class AcceptStreamHandler extends DefaultStreamHandler implements AcceptTransitionListener
{
    private final MessageConsumer acceptThrottle;

    private final long acceptStreamId;
    private final long acceptSourceRef;
    private final String acceptSourceName;
    private final long acceptCorrelationId;

    private long acceptReplyStreamId;

    private MessageConsumer connectTarget;
    private long connectId;

    private MessageConsumer streamState;

    private int slotIndex = NO_SLOT;
    private int slotOffset;

    private int acceptWindowBytes;
    private int acceptWindowFrames;

    private int sourceWindowBytesAdjustment;
    private int sourceWindowFramesAdjustment;

    MessageConsumer acceptReply;

    // One instance per Stream
    AcceptStreamHandler(
        Context context,
        MessageConsumer acceptThrottle,
        long acceptStreamId,
        long acceptSourceRef,
        String acceptSourceName,
        long acceptCorrelationId)
    {
        super(context);
        // init state machine
        this.streamState = this::beforeBeginState;

        // save reusable data (eventually mode to context)
        this.acceptThrottle = acceptThrottle;
        this.acceptStreamId = acceptStreamId;
        this.acceptSourceRef = acceptSourceRef;
        this.acceptSourceName = acceptSourceName;
        this.acceptCorrelationId = acceptCorrelationId;
    }


    RouteFW resolveTarget(
        long sourceRef,
        String sourceName)
    {
        return context.router.resolve
            (
                (msgTypeId, buffer, offset, limit) ->
                {
                    RouteFW route = context.routeRO.wrap(buffer, offset, limit);
                    // TODO implement mode and destination
                    return sourceRef == route.sourceRef() &&
                        sourceName.equals(route.source()
                            .asString());
                },
                (msgTypeId, buffer, offset, length) ->
                {
                    return context.routeRO.wrap(buffer, offset, offset + length);
                }
            );
    }

    void handleStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        System.out.println(this.getClass() + " handleStream");
        streamState.accept(msgTypeId, buffer, index, length);
    }

    @State
    private void beforeBeginState(
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
            doReset(acceptThrottle, acceptStreamId);
        }
    }

    private void handleBegin(
        BeginFW begin)
    {
        System.out.println(this.getClass() + " handleBegin");
        // TODO confirm this is same BeginFW instance as the one used to create the current AcceptStreamHandler
        final RouteFW connectRoute = resolveTarget(acceptSourceRef, acceptSourceName);
        final String connectName = connectRoute.target()
            .asString();
        final MessageConsumer connect = context.router.supplyTarget(connectName);
        final long connectRef = connectRoute.targetRef();
        final long connectStreamId = context.supplyStreamId.getAsLong();
        final long connectCorrelationId = context.supplyCorrelationId.getAsLong();



        final Correlation correlation = new Correlation();
        correlation.acceptCorrelationId(begin.correlationId());
        correlation.acceptName(acceptSourceName);
        correlation.connectStreamId(connectStreamId);
        correlation.acceptTransitionListener(this);

        context.correlations.put(connectCorrelationId, correlation);
        final BeginFW connectBegin = context.beginRW
            .wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
            .streamId(connectStreamId)
            .source("socks")
            .sourceRef(connectRef)
            .correlationId(connectCorrelationId)
            .extension(e -> e.reset())
            .build();
        connect.accept(connectBegin.typeId(), connectBegin.buffer(), connectBegin.offset(), connectBegin.sizeof());


        // doWindow(this::handleAcceptReplyThrottle, connectStreamId, 1024, 1024); // TODO replace hardcoded values
        context.router.setThrottle(connectName, connectStreamId, this::handleConnectThrottle); // FIXME handleAcceptReplyThrottle ?
        this.streamState = this::beforeConnectState;
    }


    @Override
    public void transitionToConnectionReady()
    {
        // checkState();
        // TODO send the BeginStream back on the AcceptReplyStream
        // doSendBeginStream
        // tell accept stream you can handle more data
        this.doWindow(acceptThrottle, acceptStreamId, 1024, 1024); // TODO replace hardcoded values
        this.streamState = this::afterConnectState;
    }

    @Override
    public void transitionToAborted()
    {

    }

    @State
    private void beforeConnectState(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        doReset(acceptThrottle, acceptStreamId);
    }

    @State
    private void afterConnectState(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            final DataFW data = context.dataRO.wrap(buffer, index, index + length);
            //            handleConnectRequestData(data);
            break;
        case EndFW.TYPE_ID:
        case AbortFW.TYPE_ID:
            doAbort(acceptReply, acceptReplyStreamId);
            break;
        default:
            doReset(acceptThrottle, acceptStreamId);
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
            final WindowFW window = context.windowRO.wrap(buffer, index, index + length);
            handleWindow(window);
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

    private void handleConnectThrottle(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case WindowFW.TYPE_ID:
            final WindowFW window = context.windowRO.wrap(buffer, index, index + length);
            handleWindow(window); // FIXME handle connect Window
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = context.resetRO.wrap(buffer, index, index + length);
            handleReset(reset);  // FIXME handle connect Reset
            break;
        default:
            // ignore
            break;
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
            doWindow(acceptThrottle, acceptStreamId, Math.max(sourceWindowBytesDelta, 0), Math.max(sourceWindowFramesDelta, 0));
        }
    }

    private void handleReset(
        ResetFW reset)
    {
        doReset(acceptThrottle, acceptStreamId);
    }
}
