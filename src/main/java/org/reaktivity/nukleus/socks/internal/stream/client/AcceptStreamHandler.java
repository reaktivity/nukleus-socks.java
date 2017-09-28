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

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.socks.internal.metadata.State;
import org.reaktivity.nukleus.socks.internal.stream.AbstractStreamHandler;
import org.reaktivity.nukleus.socks.internal.stream.AcceptTransitionListener;
import org.reaktivity.nukleus.socks.internal.stream.Context;
import org.reaktivity.nukleus.socks.internal.stream.Correlation;
import org.reaktivity.nukleus.socks.internal.types.control.RouteFW;
import org.reaktivity.nukleus.socks.internal.types.control.SocksRouteExFW;
import org.reaktivity.nukleus.socks.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.socks.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.socks.internal.types.stream.DataFW;
import org.reaktivity.nukleus.socks.internal.types.stream.EndFW;
import org.reaktivity.nukleus.socks.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.socks.internal.types.stream.WindowFW;

public final class AcceptStreamHandler extends AbstractStreamHandler implements AcceptTransitionListener
{
    // Can be used to send RESET and WINDOW back to the SOURCE on the ACCEPT stream
    private final MessageConsumer acceptThrottle;

    // ACCEPT stream identifier
    private final long acceptStreamId;

    // ACCEPT SOURCE endpoint metadata
    private final long acceptSourceRef;
    private final String acceptSourceName;

    // Current handler of incoming BEGIN, DATA, END, ABORT frames on the ACCEPT stream
    private MessageConsumer acceptHandlerState;

    // Can be used to send BEGIN, DATA, END, ABORT frames to the SOURCE on the ACCEPT-REPLY stream
    private MessageConsumer acceptReplyEndpoint;

    // Can be used to send BEGIN, DATA, END, ABORT frames to the TARGET on the CONNECT stream
    private MessageConsumer connectEndpoint;

    // CONNECT stream identifier
    private long connectStreamId;

    // CONNECT TARGET endpoint metadata
    private long connectTargetRef;

    private final long acceptCorrelationId;  // TODO use or remove
    private long acceptReplyStreamId;        // TODO use or remove


    /* Start of Window */
    private int acceptWindowBytes;
    private int acceptWindowFrames;

    private int sourceWindowBytesAdjustment;
    private int sourceWindowFramesAdjustment;
    /* End of Window */


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
        this.acceptHandlerState = this::beforeBeginState;

        // save reusable data (eventually move to context, since it will not change at all)
        this.acceptThrottle = acceptThrottle;
        this.acceptStreamId = acceptStreamId;
        this.acceptSourceRef = acceptSourceRef;
        this.acceptSourceName = acceptSourceName;
        this.acceptCorrelationId = acceptCorrelationId;
    }

    @Override
    protected void handleStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        acceptHandlerState.accept(msgTypeId, buffer, index, length);
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
        acceptReplyEndpoint = context.router.supplyTarget(acceptSourceName);

        // TODO confirm this is same BeginFW instance as the one used to create the current AcceptStreamHandler
        final RouteFW connectRoute = resolveTarget(acceptSourceRef, acceptSourceName);
        final String connectName = connectRoute.target().asString();
        connectEndpoint = context.router.supplyTarget(connectName);
        connectTargetRef = connectRoute.targetRef();
        connectStreamId = context.supplyStreamId.getAsLong();
        final long connectCorrelationId = context.supplyCorrelationId.getAsLong();

        final Correlation correlation = new Correlation();
        correlation.acceptCorrelationId(this.acceptCorrelationId);
        correlation.acceptName(acceptSourceName);
        correlation.connectStreamId(connectStreamId);
        correlation.acceptTransitionListener(this);
        correlation.connectRef(connectTargetRef);

        context.correlations.put(connectCorrelationId, correlation);
        final BeginFW connectBegin = context.beginRW
            .wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
            .streamId(connectStreamId)
            .source(NUKLEUS_SOCKS_NAME)
            .sourceRef(connectTargetRef)
            .correlationId(connectCorrelationId)
            .extension(e -> e.reset())
            .build();
        connectEndpoint.accept(connectBegin.typeId(), connectBegin.buffer(), connectBegin.offset(), connectBegin.sizeof());

        // FIXME confirm this is the way to handle theottling after sending a message on the CONNECT
        context.router.setThrottle(connectName, connectStreamId, this::handleConnectThrottle);
        this.acceptHandlerState = this::beforeConnectState;
    }

    @Override
    public void transitionToConnectionReady()
    {
        // TODO checkState();
        // this.doWindow(acceptThrottle, acceptStreamId, 1024, 1024); // TODO replace hardcoded values
        this.acceptHandlerState = this::afterConnectState;
    }

    @Override
    public void transitionToAborted()
    {
        // TODO
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
            handleHighLevelData(data);
            break;
        case EndFW.TYPE_ID:
        case AbortFW.TYPE_ID:
            doAbort(acceptReplyEndpoint, acceptReplyStreamId);
            break;
        default:
            doReset(acceptThrottle, acceptStreamId);
            break;
        }
    }

    private void handleHighLevelData(DataFW data)
    {
        DataFW dataForwardFW = context.dataRW.wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
            .streamId(connectStreamId)
            .payload(p -> p.set(
                data.payload().buffer(),
                data.payload().offset(),
                data.payload().sizeof()))
            .extension(e -> e.reset())
            .build();
        connectEndpoint.accept(
            dataForwardFW.typeId(),
            dataForwardFW.buffer(),
            dataForwardFW.offset(),
            dataForwardFW.sizeof());

        // this.doWindow(acceptThrottle, acceptStreamId, 1024, 1024); // TODO replace hardcoded values
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
        // TODO
    }

    private void handleAbort(
        AbortFW abort)
    {
        // TODO
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


    private RouteFW resolveTarget(
        long sourceRef,
        String sourceName)
    {
        return context.router.resolve(
            (msgTypeId, buffer, offset, limit) ->
            {
                RouteFW route = context.routeRO.wrap(buffer, offset, limit);
                final SocksRouteExFW routeEx = route.extension().get(context.routeExRO::wrap);
                return sourceRef == route.sourceRef() &&
                    sourceName.equals(route.source()
                        .asString()) && "FORWARD".equalsIgnoreCase(routeEx.mode().toString());
            },
            (msgTypeId, buffer, offset, length) ->
            {
                return context.routeRO.wrap(buffer, offset, offset + length);
            }
        );
    }
}
