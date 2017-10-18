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

import java.util.Optional;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.socks.internal.metadata.Signal;
import org.reaktivity.nukleus.socks.internal.metadata.State;
import org.reaktivity.nukleus.socks.internal.stream.AbstractStreamProcessor;
import org.reaktivity.nukleus.socks.internal.stream.AcceptTransitionListener;
import org.reaktivity.nukleus.socks.internal.stream.Context;
import org.reaktivity.nukleus.socks.internal.stream.Correlation;
import org.reaktivity.nukleus.socks.internal.stream.types.Fragmented;
import org.reaktivity.nukleus.socks.internal.stream.types.SocksCommandRequestFW;
import org.reaktivity.nukleus.socks.internal.stream.types.SocksNegotiationRequestFW;
import org.reaktivity.nukleus.socks.internal.types.OctetsFW;
import org.reaktivity.nukleus.socks.internal.types.control.RouteFW;
import org.reaktivity.nukleus.socks.internal.types.control.SocksRouteExFW;
import org.reaktivity.nukleus.socks.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.socks.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.socks.internal.types.stream.DataFW;
import org.reaktivity.nukleus.socks.internal.types.stream.EndFW;
import org.reaktivity.nukleus.socks.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.socks.internal.types.stream.WindowFW;

public final class AcceptStreamProcessor extends AbstractStreamProcessor implements AcceptTransitionListener
{

    // Current handler of incoming BEGIN, DATA, END, ABORT frames on the ACCEPT stream
    private MessageConsumer acceptProcessorState;

    private final Correlation correlation;

    private int connectWindowBytes = 0;
    private int connectWindowFrames = 0;

    boolean isConnectReplyStateReady = false;

    // One instance per Stream
    AcceptStreamProcessor(
        Context context,
        MessageConsumer acceptThrottle,
        long acceptStreamId,
        long acceptSourceRef,
        String acceptSourceName,
        long acceptCorrelationId)
    {
        super(context);
        // init state machine
        acceptProcessorState = this::beforeBegin;

        final RouteFW tmpConnectRoute = resolveTarget(acceptSourceRef, acceptSourceName);
        final String tmpConnectTargetName = tmpConnectRoute.target().asString();
        correlation = new Correlation();
        correlation.acceptThrottle(acceptThrottle);
        correlation.acceptStreamId(acceptStreamId);
        correlation.acceptSourceRef(acceptSourceRef);
        correlation.acceptSourceName(acceptSourceName);
        correlation.acceptCorrelationId(acceptCorrelationId);
        correlation.acceptReplyStreamId(context.supplyStreamId.getAsLong());
        correlation.acceptReplyEndpoint(context.router.supplyTarget(acceptSourceName));
        correlation.acceptTransitionListener(this);
        correlation.connectRoute(tmpConnectRoute);
        correlation.connectTargetName(tmpConnectRoute.target()
            .asString());
        correlation.connectEndpoint(context.router.supplyTarget(tmpConnectTargetName));
        correlation.connectTargetRef(tmpConnectRoute.targetRef());
        correlation.connectStreamId(context.supplyStreamId.getAsLong());
        correlation.connectCorrelationId(context.supplyCorrelationId.getAsLong());
        correlation.nextAcceptSignal(this::attemptNegotiationRequest);
    }

    @Override
    protected void handleStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        acceptProcessorState.accept(msgTypeId, buffer, index, length);
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
        // Store the correlation for reuse in the ConnectReplyStreamProcessor
        context.correlations.put(correlation.connectCorrelationId(), correlation);
        // Lazy initialization of CONNECT throttling handler
        context.router.setThrottle(
            correlation.connectTargetName(),
            correlation.connectStreamId(),
            this::handleConnectThrottleBeforeHandshake);
        // Initiate the stream to the TARGET
        final BeginFW connectBegin = context.beginRW
            .wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
            .streamId(correlation.connectStreamId())
            .source(NUKLEUS_SOCKS_NAME)
            .sourceRef(correlation.connectTargetRef())
            .correlationId(correlation.connectCorrelationId())
            .extension(e -> e.reset())
            .build();
        correlation
            .connectEndpoint()
            .accept(
                connectBegin.typeId(),
                connectBegin.buffer(),
                connectBegin.offset(),
                connectBegin.sizeof());
        this.acceptProcessorState = this::beforeConnect;
    }

    @State
    private void beforeConnect(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
    }

    @State
    private void afterConnect(
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
            doEnd(correlation.acceptReplyEndpoint(), correlation.acceptReplyStreamId());
            break;
        case AbortFW.TYPE_ID:
            doAbort(correlation.acceptReplyEndpoint(), correlation.acceptReplyStreamId());
            doAbort(correlation.connectEndpoint(), correlation.connectStreamId());
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
        System.out.println("Sending data: " + dataForwardFW + " to stream: " + correlation.connectEndpoint());
        correlation.connectEndpoint()
            .accept(
                dataForwardFW.typeId(),
                dataForwardFW.buffer(),
                dataForwardFW.offset(),
                dataForwardFW.sizeof());
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
            System.out.println("ACCEPT/handleConnectThrottleAfterHandshake");
            doWindow(
                correlation.acceptThrottle(),
                correlation.acceptStreamId(),
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
            correlation.nextAcceptSignal().accept(isConnectReplyStateReady);
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

    private void handleReset(
        ResetFW reset)
    {
        doReset(
            correlation.acceptThrottle(),
            correlation.acceptStreamId()
        );
    }

    @Signal
    public void attemptNegotiationRequest(boolean isConnectReplyBeginFrameReceived)
    {
        SocksNegotiationRequestFW socksNegotiationRequestFW = context.socksNegotiationRequestRW
            .wrap(context.writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, context.writeBuffer.capacity())
            .version((byte) 0x05)
            .nmethods((byte) 0x01)
            .method(new byte[]{0x00})
            .build();
        DataFW dataNegotiationRequestFW = context.dataRW.wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
            .streamId(correlation.connectStreamId())
            .payload(p -> p.set(
                socksNegotiationRequestFW.buffer(),
                socksNegotiationRequestFW.offset(),
                socksNegotiationRequestFW.sizeof()))
            .extension(e -> e.reset())
            .build();

        if (connectWindowFrames > 0 &&
            connectWindowBytes > dataNegotiationRequestFW.sizeof())
        {
            correlation.connectEndpoint()
                .accept(
                    dataNegotiationRequestFW.typeId(),
                    dataNegotiationRequestFW.buffer(),
                    dataNegotiationRequestFW.offset(),
                    dataNegotiationRequestFW.sizeof());

            connectWindowBytes -= dataNegotiationRequestFW.sizeof();
            connectWindowFrames--;
            correlation.nextAcceptSignal(this::attemptConnectionRequest);
        }
    }

    @Signal
    public void attemptConnectionRequest(boolean isNegotiationResponseReceived)
    {
        if (!(this.isConnectReplyStateReady = isNegotiationResponseReceived))
        {
            return;
        }
        final RouteFW connectRoute = correlation.connectRoute();
        final SocksRouteExFW routeEx = connectRoute.extension().get(context.routeExRO::wrap);
        String destAddrPort = routeEx.destAddrPort().asString();
        // Reply with Socks version 5 and "NO AUTHENTICATION REQUIRED"
        SocksCommandRequestFW socksConnectRequestFW = context.socksConnectionRequestRW
            .wrap(context.writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, context.writeBuffer.capacity())
            .version((byte) 0x05)
            .command((byte) 0x01) // CONNECT
            .destination(destAddrPort)
            .build();
        if (socksConnectRequestFW.getBuildState() == Fragmented.BuildState.BROKEN)
        {
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
            return;
        }
        DataFW dataConnectRequestFW = context.dataRW.wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
            .streamId(correlation.connectStreamId())
            .payload(p -> p.set(
                socksConnectRequestFW.buffer(),
                socksConnectRequestFW.offset(),
                socksConnectRequestFW.sizeof()))
            .extension(e -> e.reset())
            .build();
        if (connectWindowFrames > 0 &&
            connectWindowBytes > dataConnectRequestFW.sizeof())
        {
            correlation.connectEndpoint().accept(
                dataConnectRequestFW.typeId(),
                dataConnectRequestFW.buffer(),
                dataConnectRequestFW.offset(),
                dataConnectRequestFW.sizeof());
            connectWindowBytes -= dataConnectRequestFW.sizeof();
            connectWindowFrames--;
            correlation.nextAcceptSignal(this::noop);
        }
    }

    /*
     * Can be called if we receive WINDOW frames between:
     *     - sending the ConnectRequest
     *     - receiving the ConnectReply
     */
    @Signal
    public void noop(boolean isConnectReplyStateReady)
    {
    }

    @Override
    public void transitionToConnectionReady(Optional connectionInfo)
    {
        this.acceptProcessorState = this::afterConnect;
        context.router.setThrottle(
            correlation.connectTargetName(),
            correlation.connectStreamId(),
            this::handleConnectThrottleAfterHandshake);
        System.out.println("ACCEPT/transitionToConnectionReady");
        this.doWindow(
            correlation.acceptThrottle(),
            correlation.acceptStreamId(),
            connectWindowBytes,
            connectWindowFrames);
    }

    @Override
    public void transitionToAborted()
    {
        this.acceptProcessorState = this::afterAbort;
        doAbort(correlation.connectEndpoint(), correlation.connectStreamId());
    }

    @State
    private void afterAbort(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
    }

    private RouteFW resolveTarget(
        long sourceRef,
        String sourceName)
    {
        return context.router.resolve(
            (msgTypeId, buffer, offset, limit) ->
            {
                RouteFW route = context.routeRO.wrap(buffer, offset, limit);
                return sourceRef == route.sourceRef() &&
                    sourceName.equals(route.source()
                        .asString());
            },
            (msgTypeId, buffer, offset, length) ->
            {
                return context.routeRO.wrap(buffer, offset, offset + length);
            });
    }
}