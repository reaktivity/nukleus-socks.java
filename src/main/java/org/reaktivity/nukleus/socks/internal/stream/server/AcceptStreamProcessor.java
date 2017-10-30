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

import java.util.Optional;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.socks.internal.metadata.Signal;
import org.reaktivity.nukleus.socks.internal.metadata.State;
import org.reaktivity.nukleus.socks.internal.stream.AbstractStreamProcessor;
import org.reaktivity.nukleus.socks.internal.stream.AcceptTransitionListener;
import org.reaktivity.nukleus.socks.internal.stream.Context;
import org.reaktivity.nukleus.socks.internal.stream.Correlation;
import org.reaktivity.nukleus.socks.internal.stream.types.FragmentedFlyweight;
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

public final class AcceptStreamProcessor extends AbstractStreamProcessor implements AcceptTransitionListener<TcpBeginExFW>
{
    public static final int MAX_WRITABLE_BYTES = Integer.parseInt(System.getProperty("socks.initial.window", "65536"));

    private MessageConsumer streamState;
    private MessageConsumer acceptReplyThrottleState;
    private MessageConsumer connectThrottleState;

    private int acceptReplyCredit;
    private int acceptReplyPadding;

    private int connectCredit;
    private int connectPadding;

    private int receivedAcceptBytes;

    private byte socksAtyp;
    private byte[] socksAddr;
    private int socksPort;

    final Correlation correlation;

    public AcceptStreamProcessor(
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
        MessageConsumer acceptReplyEndpoint = context.router.supplyTarget(acceptSourceName);
        correlation = new Correlation(
            acceptThrottle,
            acceptStreamId,
            acceptSourceRef,
            acceptSourceName,
            acceptReplyStreamId,
            acceptCorrelationId,
            this
        );
        correlation.acceptReplyEndpoint(acceptReplyEndpoint);
        correlation.connectStreamId(context.supplyStreamId.getAsLong());
        correlation.connectCorrelationId(context.supplyCorrelationId.getAsLong());
        correlation.nextAcceptSignal(this::noop);
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
            initializeNegotiation(begin);
        }
        else
        {
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
        }
    }

    private void initializeNegotiation(
        BeginFW begin)
    {
        doBegin(
            correlation.acceptReplyEndpoint(),
            correlation.acceptReplyStreamId(),
            0L,
            correlation.acceptCorrelationId());
        doWindow(
            correlation.acceptThrottle(),
            correlation.acceptStreamId(),
            MAX_WRITABLE_BYTES,
            0);
        this.streamState = this::afterNegotiationInitialized;
    }

    @State
    private void afterNegotiationInitialized(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            final DataFW data = context.dataRO.wrap(buffer, index, index + length);
            receivedAcceptBytes += data.payload().sizeof();
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
        handleFragmentedData(
            data,
            context.socksNegotiationRequestRO,
            this::handleNegotiationFlyweight,
            correlation.acceptThrottle(),
            correlation.acceptStreamId());
    }

    private void handleNegotiationFlyweight(
        FragmentedFlyweight<SocksNegotiationRequestFW> flyweight,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        final SocksNegotiationRequestFW socksNegotiation = flyweight.wrap(buffer, offset, limit);
        if (socksNegotiation.version() != 0x05)
        {
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
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
            correlation.nextAcceptSignal(this::attemptFailedNegotiationResponse);
            correlation.nextAcceptSignal().accept(true);
        }
        else
        {
            correlation.nextAcceptSignal(this::attemptNegotiationResponse);
            correlation.nextAcceptSignal().accept(true);
        }
    }

    private void updatePartial(int sentBytesWithPadding)
    {
        acceptReplyCredit -= sentBytesWithPadding;
    }

    private void updateNegotiationResponseComplete(int sentBytesWithPadding)
    {
        this.streamState = this::afterNegotiation;
        correlation.nextAcceptSignal(this::noop);
        acceptReplyCredit -= sentBytesWithPadding;
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
        doFragmentedData(socksNegotiationResponseFW,
            acceptReplyCredit,
            acceptReplyPadding,
            correlation.acceptReplyEndpoint(),
            correlation.acceptReplyStreamId(),
            this::updatePartial,
            this::updateNegotiationResponseComplete);
    }

    private void updateNegotiationFailedResponseComplete(int sentBytesWithPadding)
    {
        this.streamState = this::afterFailedHandshake;
        correlation.nextAcceptSignal(this::noop);
        acceptReplyCredit -= sentBytesWithPadding;
    }

    @Signal
    private void attemptFailedNegotiationResponse(
        boolean isReadyState)
    {
        SocksNegotiationResponseFW socksNegotiationResponseFW = context.socksNegotiationResponseRW
            .wrap(context.writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, context.writeBuffer.capacity())
            .version((byte) 0x05)
            .method((byte) 0xFF)
            .build();
        doFragmentedData(socksNegotiationResponseFW,
            acceptReplyCredit,
            acceptReplyPadding,
            correlation.acceptReplyEndpoint(),
            correlation.acceptReplyStreamId(),
            this::updatePartial,
            this::updateNegotiationFailedResponseComplete);
    }

    @State
    private void afterFailedHandshake(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        // Only EndFrame should come back
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
            receivedAcceptBytes += data.payload().sizeof();
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

    private void handleConnectRequestFlyweight(
        FragmentedFlyweight<SocksCommandRequestFW> flyweight,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        final SocksCommandRequestFW socksCommandRequestFW = flyweight.wrap(buffer, offset, limit);
        if (socksCommandRequestFW.version() != 0x05)
        {
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
        }
        if (socksCommandRequestFW.command() != 0x01)
        {
            correlation.nextAcceptSignal(this::attemptFailedConnectionResponse);
            correlation.nextAcceptSignal().accept(true);
        }
        else
        {
            final String dstAddrPort = socksCommandRequestFW.getDstAddrPort();
            final RouteFW connectRoute = resolveTarget(
                correlation.acceptSourceRef(),
                correlation.acceptSourceName(),
                dstAddrPort);
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
                this::handleConnectThrottle);
            doBegin(
                connectEndpoint,
                connectStreamId,
                connectTargetRef,
                connectCorrelationId);
            correlation.nextAcceptSignal(this::noop);
            this.streamState = this::afterTargetConnectBegin;
        }
    }

    private void handleConnectRequestData(
        DataFW data)
    {
        handleFragmentedData(
            data,
            context.socksConnectionRequestRO,
            this::handleConnectRequestFlyweight,
            correlation.acceptThrottle(),
            correlation.acceptStreamId());
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
    private void attemptFailedConnectionResponse(
        boolean isReadyState)
    {
        if (!isReadyState)
        {
            correlation.nextAcceptSignal(this::attemptFailedConnectionResponse);
        }
        SocksCommandResponseFW socksConnectResponseFW = context.socksConnectionResponseRW
            .wrap(context.writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, context.writeBuffer.capacity())
            .version((byte) 0x05)
            .reply((byte) 0x07) // COMMAND NOT SUPPORTED
            .bind((byte) 0x01, new byte[]{0x00, 0x00, 0x00, 0x00}, 0x00)
            .build();
        doFragmentedData(socksConnectResponseFW,
            acceptReplyCredit,
            acceptReplyPadding,
            correlation.acceptReplyEndpoint(),
            correlation.acceptReplyStreamId(),
            this::updatePartial,
            this::updateConnectionFailedResponseComplete);
    }

    private void updateConnectionFailedResponseComplete(int sentBytesWithPadding)
    {
        acceptReplyCredit -= sentBytesWithPadding;
        this.streamState = this::afterFailedHandshake;
        correlation.nextAcceptSignal(this::noop);
    }

    private void updateConnectionResponseComplete(int sentBytesWithPadding)
    {
        correlation.nextAcceptSignal(this::noop);
        acceptReplyCredit -= sentBytesWithPadding;

        // Optimistic case, the frames can be forwarded back and forth
        this.streamState = this::afterSourceConnect;
        this.acceptReplyThrottleState = this::handleAcceptReplyThrottleAfterHandshake;
        this.connectThrottleState = this::handleConnectThrottleAfterHandshake;
        doWindow(
            correlation.connectReplyThrottle(),
            correlation.connectReplyStreamId(),
            acceptReplyCredit,
            acceptReplyPadding);
        if (isConnectWindowGreaterThanAcceptWindow())
        {
            doWindow(
                correlation.acceptThrottle(),
                correlation.acceptStreamId(),
                connectCredit - receivedAcceptBytes - MAX_WRITABLE_BYTES,
                connectPadding);
        }
        else
        {
            this.connectThrottleState = this::handleConnectThrottleBufferUnwind;
            this.streamState = this::afterSourceConnectBufferUnwind;
        }
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
        doFragmentedData(socksConnectResponseFW,
            acceptReplyCredit,
            acceptReplyPadding,
            correlation.acceptReplyEndpoint(),
            correlation.acceptReplyStreamId(),
            this::updatePartial,
            this::updateConnectionResponseComplete);
    }

    @State
    private void afterSourceConnectBufferUnwind(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            final DataFW data = context.dataRO.wrap(buffer, index, index + length);
            receivedAcceptBytes += data.payload().sizeof();
            handleHighLevelDataBufferUnwind(
                data,
                getCurrentTargetCredit(),
                correlation.connectStreamId(),
                correlation.acceptThrottle(),
                correlation.acceptStreamId(),
                this::updateSentPartialData,
                this::updateSentCompleteData);
            break;
        case EndFW.TYPE_ID:
        case AbortFW.TYPE_ID:
            doAbort(correlation.acceptReplyEndpoint(), correlation.acceptReplyStreamId());
            break;
        default:
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
        }
    }

    private void updateSentPartialData(
        OctetsFW payload,
        int payloadLength)
    {
        doForwardData(
            payload,
            payloadLength,
            correlation.connectStreamId(),
            correlation.connectEndpoint());
        connectCredit = 0;
    }

    private void updateSentCompleteData(OctetsFW payload)
    {
        doForwardData(
            payload,
            correlation.connectStreamId(),
            correlation.connectEndpoint());
        connectCredit -= payload.sizeof() + connectPadding;
        if (connectCredit == 0)
        {
            this.connectThrottleState = this::handleConnectThrottleAfterHandshake;
            this.streamState = this::afterSourceConnect;
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
            doEnd(correlation.acceptReplyEndpoint(), correlation.acceptReplyStreamId());
            break;
        case AbortFW.TYPE_ID:
            doAbort(correlation.acceptReplyEndpoint(), correlation.acceptReplyStreamId());
            break;
        default:
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
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
            acceptReplyCredit += window.credit();
            acceptReplyPadding = window.padding();
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
                window.credit(),
                window.padding());
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = context.resetRO.wrap(buffer, index, index + length);
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
            doAbort(correlation.connectEndpoint(), correlation.connectStreamId());
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
            connectCredit += window.credit();
            connectPadding = window.padding();
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

    private void handleConnectThrottleBufferUnwind(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case WindowFW.TYPE_ID:
            final WindowFW window = context.windowRO.wrap(buffer, index, index + length);
            connectCredit += window.credit();
            connectPadding = window.padding();
            handleThrottlingAndDataBufferUnwind(
                getCurrentTargetCredit(),
                this::updateSendDataFromBuffer,
                this::updateThrottlingWithEmptyBuffer);
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = context.resetRO.wrap(buffer, index, index + length);
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
            doAbort(correlation.acceptReplyEndpoint(), correlation.acceptReplyStreamId());
            doReset(correlation.connectReplyThrottle(), correlation.connectReplyStreamId());
            break;
        default:
            // ignore
            break;
        }
    }

    private void updateThrottlingWithEmptyBuffer(boolean shouldForwardSourceWindow)
    {
        if (isConnectWindowGreaterThanAcceptWindow())
        {
            this.connectThrottleState = this::handleConnectThrottleAfterHandshake;
            this.streamState = this::afterSourceConnect;
            if (shouldForwardSourceWindow)
            {
                doWindow(
                    correlation.acceptThrottle(),
                    correlation.acceptStreamId(),
                    receivedAcceptBytes,
                    connectPadding);
            }
        }
    }

    private void updateSendDataFromBuffer(
        MutableDirectBuffer acceptBuffer,
        int payloadSize)
    {
        doForwardData(
            acceptBuffer,
            0,
            payloadSize,
            correlation.connectStreamId(),
            correlation.connectEndpoint());
        connectCredit -= payloadSize + connectPadding;
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
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
            doAbort(correlation.acceptReplyEndpoint(), correlation.acceptReplyStreamId());
            doReset(correlation.connectReplyThrottle(), correlation.connectReplyStreamId());
            break;
        default:
            // ignore
            break;
        }
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
        return context.router.resolve(0L, filter, this::wrapRoute);
    }

    private RouteFW wrapRoute(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        return context.routeRO.wrap(buffer, index, index + length);
    }

    public boolean isConnectWindowGreaterThanAcceptWindow()
    {
        return connectCredit - connectPadding > MAX_WRITABLE_BYTES - receivedAcceptBytes;
    }

    private int getCurrentTargetCredit()
    {
        return connectCredit - connectPadding;
    }
}
