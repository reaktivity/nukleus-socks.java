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

import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.socks.internal.types.OctetsFW;
import org.reaktivity.nukleus.socks.internal.types.control.RouteFW;
import org.reaktivity.nukleus.socks.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.socks.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.socks.internal.types.stream.DataFW;
import org.reaktivity.nukleus.socks.internal.types.stream.EndFW;
import org.reaktivity.nukleus.socks.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.socks.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class ServerStreamFactory implements StreamFactory
{
    private final RouteFW routeRO = new RouteFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    private final OctetsFW octetsRO = new OctetsFW();

    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final BufferPool bufferPool;
    private final LongSupplier supplyStreamId;
    private final LongSupplier supplyCorrelationId;

    private final Long2ObjectHashMap<Correlation> correlations;
    private final MessageFunction<RouteFW> wrapRoute;

    public ServerStreamFactory(
        Configuration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongSupplier supplyStreamId,
        LongSupplier supplyCorrelationId,
        Long2ObjectHashMap<Correlation> correlations)
    {
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.bufferPool = requireNonNull(bufferPool);
        this.supplyStreamId = requireNonNull(supplyStreamId);
        this.supplyCorrelationId = requireNonNull(supplyCorrelationId);
        this.correlations = requireNonNull(correlations);
        this.wrapRoute = this::wrapRoute;
        System.out.println("Got the StreamFactory going....");
    }

    @Override
    public MessageConsumer newStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length,
        MessageConsumer throttle)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long sourceRef = begin.sourceRef();

        MessageConsumer newStream = null;

        if (sourceRef == 0L)
        {
            System.out.println("newConnectReplyStream");
            newStream = newConnectReplyStream(begin, throttle);
        }
        else
        {
            System.out.println("newAcceptStream");
            newStream = newAcceptStream(begin, throttle);
        }

        return newStream;
    }

    private MessageConsumer newAcceptStream(
        final BeginFW begin,
        final MessageConsumer acceptThrottle)
    {
        final long acceptRef = begin.sourceRef();
        final String acceptName = begin.source().asString();

        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, l);
            return acceptRef == route.sourceRef() &&
                    acceptName.equals(route.source().asString());
        };

        final RouteFW route = router.resolve(filter, this::wrapRoute);

        MessageConsumer newStream = null;

        if (route != null)
        {
            final long acceptId = begin.streamId();
            System.out.println("Creating new Server accept Stream");
            newStream = new ServerAcceptStream(acceptThrottle, acceptId, acceptRef)::handleStream;
        }

        return newStream;
    }

    private MessageConsumer newConnectReplyStream(
        final BeginFW begin,
        final MessageConsumer connectReplyThrottle)
    {
        final long connectReplyId = begin.streamId();

        return new ServerConnectReplyStream(connectReplyThrottle, connectReplyId)::handleStream;
    }

    private RouteFW wrapRoute(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        System.out.println("WrapRoute");
        return routeRO.wrap(buffer, index, index + length);
    }

    private final class ServerAcceptStream
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

        private ServerAcceptStream(
            MessageConsumer acceptThrottle,
            long acceptId,
            long acceptRef)
        {
            this.acceptThrottle = acceptThrottle;
            this.acceptId = acceptId;

            this.streamState = this::beforeBegin;
        }

        private void handleStream(
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
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                handleBegin(begin);
            }
            else
            {
                doReset(acceptThrottle, acceptId);
            }
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
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                handleData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                handleEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                handleAbort(abort);
                break;
            default:
                doReset(acceptThrottle, acceptId);
                break;
            }
        }

        private void handleBegin(
            BeginFW begin)
        {
            final String acceptName = begin.source().asString();
            final long acceptRef = begin.sourceRef();
            final long correlationId = begin.correlationId();
            final OctetsFW extension = begin.extension();

//            // TODO: need lightweight approach (start)










//            final Map<String, String> headers = new LinkedHashMap<>();
//
//            final String version = headers.get("sec-websocket-version");
//            final String key = headers.get("sec-websocket-key");
//            final String protocols = headers.get("sec-websocket-protocol");
//            // TODO: need lightweight approach (end)
//
//                doReset(acceptThrottle, acceptId); // 404

            this.streamState = this::afterBegin;
        }

        private void handleData(
            DataFW data)
        {
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

    private final class ServerConnectReplyStream
    {
        private final MessageConsumer connectReplyThrottle;
        private final long connectReplyId;

        private MessageConsumer acceptReply;
        private long acceptReplyId;

        private MessageConsumer streamState;

        private int targetWindowBytes;
        private int targetWindowFrames;
        private int targetWindowBytesAdjustment;
        private int targetWindowFramesAdjustment;

        private Consumer<WindowFW> windowHandler;

        private ServerConnectReplyStream(
            MessageConsumer connectReplyThrottle,
            long connectReplyId)
        {
            this.connectReplyThrottle = connectReplyThrottle;
            this.connectReplyId = connectReplyId;
            this.streamState = this::beforeBegin;
        }

        private void handleStream(
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
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                handleBegin(begin);
            }
            else
            {
                doReset(connectReplyThrottle, connectReplyId);
            }
        }

        private void afterBeginOrData(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                handleData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                handleEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                handleAbort(abort);
                break;
            default:
                doReset(connectReplyThrottle, connectReplyId);
                break;
            }
        }

        private void handleBegin(
            BeginFW begin)
        {
            final long connectRef = begin.sourceRef();
            final long correlationId = begin.correlationId();

            final Correlation correlation = correlations.remove(correlationId);

            if (connectRef == 0L && correlation != null)
            {
                final String acceptReplyName = correlation.acceptName();

                final MessageConsumer newAcceptReply = router.supplyTarget(acceptReplyName);
                final long newAcceptReplyId = supplyStreamId.getAsLong();
                final long newCorrelationId = correlation.correlationId();

//                doHttpBegin(newAcceptReply, newAcceptReplyId, 0L, newCorrelationId, setHttpHeaders(handshakeHash, protocol));
                router.setThrottle(acceptReplyName, newAcceptReplyId, this::handleThrottle);

                this.acceptReply = newAcceptReply;
                this.acceptReplyId = newAcceptReplyId;

                this.streamState = this::afterBeginOrData;
                this.windowHandler = this::processInitialWindow;
            }
            else
            {
                doReset(connectReplyThrottle, connectReplyId);
            }
        }

        private void handleData(
            DataFW data)
        {
        }

        private void handleEnd(
            EndFW end)
        {
        }

        private void handleAbort(
            AbortFW abort)
        {
        }


        private void handleThrottle(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                this.windowHandler.accept(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                handleReset(reset);
                break;
            default:
                // ignore
                break;
            }
        }

        private void processInitialWindow(WindowFW window)
        {
            final int sourceWindowBytesDelta = window.update();

            targetWindowBytesAdjustment -= sourceWindowBytesDelta * 20 / 100;

            this.windowHandler = this::processWindow;
            this.windowHandler.accept(window);
        }

        private void processWindow(WindowFW window)
        {
            final int sourceWindowBytesDelta = window.update();
            final int sourceWindowFramesDelta = window.frames();

            final int targetWindowBytesDelta = sourceWindowBytesDelta + targetWindowBytesAdjustment;
            final int targetWindowFramesDelta = sourceWindowFramesDelta + targetWindowFramesAdjustment;

            targetWindowBytes += Math.max(targetWindowBytesDelta, 0);
            targetWindowBytesAdjustment = Math.min(targetWindowBytesDelta, 0);

            targetWindowFrames += Math.max(targetWindowFramesDelta, 0);
            targetWindowFramesAdjustment = Math.min(targetWindowFramesDelta, 0);

            if (targetWindowBytesDelta > 0 || targetWindowFramesDelta > 0)
            {
                doWindow(connectReplyThrottle, connectReplyId,
                        Math.max(targetWindowBytesDelta, 0), Math.max(targetWindowFramesDelta, 0));
            }
        }

        private void handleReset(
            ResetFW reset)
        {
            doReset(connectReplyThrottle, connectReplyId);
        }
    }

    private void doWsAbort(
        MessageConsumer stream,
        long streamId,
        short code)
    {
        // TODO: WsAbortEx
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(streamId)
                .extension(e -> e.reset())
                .build();

        stream.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    private void doWindow(
        final MessageConsumer throttle,
        final long throttleId,
        final int writableBytes,
        final int writableFrames)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(throttleId)
                .update(writableBytes)
                .frames(writableFrames)
                .build();

        throttle.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    private void doZeroWindow(
        final MessageConsumer throttle,
        final long throttleId)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(throttleId)
                .update(0)
                .frames(0)
                .build();

        throttle.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    private void doReset(
        final MessageConsumer throttle,
        final long throttleId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
               .streamId(throttleId)
               .build();

        throttle.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

}
