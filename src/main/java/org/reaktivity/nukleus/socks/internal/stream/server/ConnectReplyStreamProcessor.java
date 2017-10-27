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
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.socks.internal.metadata.State;
import org.reaktivity.nukleus.socks.internal.stream.AbstractStreamProcessor;
import org.reaktivity.nukleus.socks.internal.stream.Context;
import org.reaktivity.nukleus.socks.internal.stream.Correlation;
import org.reaktivity.nukleus.socks.internal.types.OctetsFW;
import org.reaktivity.nukleus.socks.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.socks.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.socks.internal.types.stream.DataFW;
import org.reaktivity.nukleus.socks.internal.types.stream.EndFW;
import org.reaktivity.nukleus.socks.internal.types.stream.TcpBeginExFW;

final class ConnectReplyStreamProcessor extends AbstractStreamProcessor
{
    private final MessageConsumer connectReplyThrottle;
    private final long connectReplyId;

    private MessageConsumer streamState;

    private Correlation correlation;

    ConnectReplyStreamProcessor(
        Context context,
        MessageConsumer connectReplyThrottle,
        long connectReplyId)
    {
        super(context);
        this.connectReplyThrottle = connectReplyThrottle;
        this.connectReplyId = connectReplyId;
        this.streamState = this::beforeBegin;
    }

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
            doReset(connectReplyThrottle, connectReplyId);
        }
    }

    private void handleBegin(BeginFW begin)
    {
        // System.out.println("CONNECT_REPLY: created stream " + begin.streamId());
        final long connectRef = begin.sourceRef();
        final long correlationId = begin.correlationId();
        correlation = context.correlations.remove(correlationId);
        if (connectRef == 0L && correlation != null)
        {
            correlation.connectReplyThrottle(connectReplyThrottle);
            correlation.connectReplyStreamId(connectReplyId);
            final TcpBeginExFW tcpBeginEx = begin.extension().get(context.tcpBeginExRO::wrap);
            Optional<TcpBeginExFW> connectionInfo = Optional.of(tcpBeginEx);
            System.out.println("CONNECT_REPLY: connection ready " + tcpBeginEx);
            System.out.println("\tacceptStreamId: " + correlation.acceptStreamId());
            System.out.println("\tacceptReplyStreamId: " + correlation.acceptReplyStreamId());
            System.out.println("\tconnectReplyStreamId: " + correlation.connectReplyStreamId());
            correlation.acceptTransitionListener().transitionToConnectionReady(connectionInfo);
            this.streamState = this::afterBeginOrData;
        }
        else
        {
            doReset(connectReplyThrottle, connectReplyId);
        }
    }

    @State
    private void afterBeginOrData(
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
        OctetsFW payload = data.payload();
        doForwardData(
            payload,
            correlation.acceptReplyStreamId(),
            correlation.acceptReplyEndpoint());
    }

    private void handleEnd(EndFW end)
    {
        EndFW endForwardFW = context.endRW
            .wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
            .streamId(correlation.acceptReplyStreamId())
            .build();
        // System.out.println("forwarding end frame: " + endForwardFW);
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
    }
}
