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

import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.socks.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.socks.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.socks.internal.types.stream.WindowFW;

class DefaultStreamHandler
{
    protected final StreamContext streamContext;

    DefaultStreamHandler(StreamContext streamContext)
    {
        this.streamContext = streamContext;
    }

    protected void doAbort(
        MessageConsumer stream,
        long streamId)
    {
        final AbortFW abort =
            streamContext.abortRW
                .wrap(streamContext.writeBuffer, 0, streamContext.writeBuffer.capacity())
                .streamId(streamId)
                .extension(e -> e.reset())
                .build();
        stream.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    protected void doWindow(
        final MessageConsumer throttle,
        final long throttleId,
        final int writableBytes,
        final int writableFrames)
    {
        final WindowFW window = streamContext.windowRW
            .wrap(streamContext.writeBuffer, 0, streamContext.writeBuffer.capacity())
            .streamId(throttleId)
            .update(writableBytes)
            .frames(writableFrames)
            .build();
        throttle.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    protected void doZeroWindow(
        final MessageConsumer throttle,
        final long throttleId)
    {
        final WindowFW window =
            streamContext.windowRW
                .wrap(streamContext.writeBuffer, 0, streamContext.writeBuffer.capacity())
                .streamId(throttleId)
                .update(0)
                .frames(0)
                .build();
        throttle.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    protected void doReset(
        final MessageConsumer throttle,
        final long throttleId)
    {
        final ResetFW reset =
            streamContext.resetRW
                .wrap(streamContext.writeBuffer, 0, streamContext.writeBuffer.capacity())
                .streamId(throttleId)
                .build();
        throttle.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }
}
