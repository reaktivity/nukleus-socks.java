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

import static org.reaktivity.nukleus.socks.internal.stream.types.SocksProtocolTypes.AUTH_METHOD_NONE;
import static org.reaktivity.nukleus.socks.internal.stream.types.SocksProtocolTypes.COMMAND_CONNECT;
import static org.reaktivity.nukleus.socks.internal.stream.types.SocksProtocolTypes.SOCKS_VERSION_5;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.socks.internal.metadata.Signal;
import org.reaktivity.nukleus.socks.internal.metadata.State;
import org.reaktivity.nukleus.socks.internal.stream.AbstractStream;
import org.reaktivity.nukleus.socks.internal.stream.AcceptTransitionListener;
import org.reaktivity.nukleus.socks.internal.stream.Context;
import org.reaktivity.nukleus.socks.internal.stream.Correlation;
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

public final class AcceptStream extends AbstractStream implements AcceptTransitionListener
{

    // Current handler of incoming BEGIN, DATA, END, ABORT frames on the ACCEPT stream
    private MessageConsumer acceptState;

    private final Correlation correlation;

    private int connectWindowCredit;
    private int connectWindowPadding;

    boolean isConnectReplyStateReady = false;

    // One instance per Stream
    AcceptStream(
        Context context,
        MessageConsumer acceptThrottle,
        long acceptStreamId,
        long acceptSourceRef,
        String acceptSourceName,
        long acceptCorrelationId)
    {
        super(context);
        // init state machine
        acceptState = this::beforeBegin;
        final RouteFW connectRoute = resolveTarget(acceptSourceRef, acceptSourceName);
        if (connectRoute == null)
        {
            correlation = null;
            acceptState = this::beforeConnect;
            doReset(acceptThrottle, acceptStreamId);
            return;
        }
        final String connectTargetName = connectRoute.target().asString();
        final long acceptReplyStreamId = context.supplyStreamId.getAsLong();
        correlation = new Correlation(
            acceptThrottle,
            acceptStreamId,
            acceptSourceRef,
            acceptSourceName,
            acceptReplyStreamId,
            acceptCorrelationId,
            this);
        correlation.nextAcceptSignal(this::attemptNegotiationRequest);
        correlation.connectRoute(connectRoute);
        correlation.connectTargetName(connectRoute.target().asString());
        correlation.connectEndpoint(context.router.supplyTarget(connectTargetName));
        correlation.connectTargetRef(connectRoute.targetRef());
        correlation.connectStreamId(context.supplyStreamId.getAsLong());
        correlation.connectCorrelationId(context.supplyCorrelationId.getAsLong());
    }

    @Override
    protected void handleStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        acceptState.accept(msgTypeId, buffer, index, length);
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
        context.correlations.put(correlation.connectCorrelationId(), correlation);
        context.router.setThrottle(
            correlation.connectTargetName(),
            correlation.connectStreamId(),
            this::handleConnectThrottleBeforeHandshake);
        doBegin(correlation.connectEndpoint(),
            correlation.connectStreamId(),
            correlation.connectTargetRef(),
            correlation.connectCorrelationId());
        this.acceptState = this::beforeConnect;
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
            doEnd(correlation.connectEndpoint(), correlation.connectStreamId());
            break;
        case AbortFW.TYPE_ID:
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
        doForwardData(
            payload,
            correlation.connectStreamId(),
            correlation.connectEndpoint());
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
            connectWindowCredit += window.credit();
            connectWindowPadding = window.padding();
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
                window.credit(),
                window.padding());
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
            correlation.acceptStreamId());
    }

    private void updatePartial(int sentBytesWithPadding)
    {
        connectWindowCredit -= sentBytesWithPadding;
    }

    private void updateNegotiationComplete(int sentBytesWithPadding)
    {
        connectWindowCredit -= sentBytesWithPadding;
        correlation.nextAcceptSignal(this::attemptConnectionRequest);
    }

    @Signal
    public void attemptNegotiationRequest(boolean isConnectReplyBeginFrameReceived)
    {
        SocksNegotiationRequestFW socksNegotiationRequestFW = context.socksNegotiationRequestRW
            .wrap(context.writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, context.writeBuffer.capacity())
            .version(SOCKS_VERSION_5)
            .nmethods((byte) 0x01)
            .method(new byte[]{AUTH_METHOD_NONE})
            .build();
        doFragmentedData(socksNegotiationRequestFW,
            connectWindowCredit,
            connectWindowPadding,
            correlation.connectEndpoint(),
            correlation.connectStreamId(),
            this::updatePartial,
            this::updateNegotiationComplete);
    }

    private void updateConnectionComplete(int sentBytesWithPadding)
    {
        connectWindowCredit -= sentBytesWithPadding;
        correlation.nextAcceptSignal(this::noop);
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

        // Reply with Socks version 5 and "NO AUTHENTICATION REQUIRED"
        SocksCommandRequestFW socksConnectRequestFW = context.socksConnectionRequestRW
            .wrap(context.writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, context.writeBuffer.capacity())
            .version(SOCKS_VERSION_5)
            .command(COMMAND_CONNECT)
            .destination(routeEx)
            .build();
        doFragmentedData(socksConnectRequestFW,
            connectWindowCredit,
            connectWindowPadding,
            correlation.connectEndpoint(),
            correlation.connectStreamId(),
            this::updatePartial,
            this::updateConnectionComplete);
    }

    @Signal
    public void noop(boolean isConnectReplyStateReady)
    {
    }

    @Override
    public void transitionToConnectionReady()
    {
        this.acceptState = this::afterConnect;
        context.router.setThrottle(
            correlation.connectTargetName(),
            correlation.connectStreamId(),
            this::handleConnectThrottleAfterHandshake);
        this.doWindow(
            correlation.acceptThrottle(),
            correlation.acceptStreamId(),
            connectWindowCredit,
            connectWindowPadding);
    }

    private RouteFW resolveTarget(
        long sourceRef,
        String sourceName)
    {
        return context.router.resolve(
            0L,
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