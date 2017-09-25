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
package org.reaktivity.nukleus.socks.internal.bench;

import static java.util.concurrent.TimeUnit.SECONDS;

import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@Fork(3)
@Warmup(iterations = 5, time = 1, timeUnit = SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = SECONDS)
@OutputTimeUnit(SECONDS)
public class SocksServerBM
{
//    private final Configuration configuration;
//    private final Reaktor reaktor;
//
//    {
//        Properties properties = new Properties();
//        properties.setProperty(DIRECTORY_PROPERTY_NAME, "target/nukleus-benchmarks");
//        properties.setProperty(STREAMS_BUFFER_CAPACITY_PROPERTY_NAME, Long.toString(1024L * 1024L * 16L));
//
//        configuration = new Configuration(properties);
//        reaktor = Reaktor.builder()
//                         .config(configuration)
//                         .nukleus("ws"::equals)
//                         .controller(SocksController.class::isAssignableFrom)
//                         .errorHandler(ex -> ex.printStackTrace(System.err))
//                         .build()
//                         .start();
//    }
//
//    private final BeginFW beginRO = new BeginFW();
//    private final DataFW dataRO = new DataFW();
//
//    private final BeginFW.Builder beginRW = new BeginFW.Builder();
//    private final DataFW.Builder dataRW = new DataFW.Builder();
//    private final WindowFW.Builder windowRW = new WindowFW.Builder();
//
//    private final HttpBeginExFW.Builder httpBeginExRW = new HttpBeginExFW.Builder();
//
//    private Source source;
//    private Target target;
//
//    private long sourceRef;
//    private long targetRef;
//
//    @Setup(Level.Trial)
//    public void reinit() throws Exception
//    {
//        final SocksController controller = reaktor.controller(SocksController.class);
//
//        final Random random = new Random();
//
//        this.targetRef = random.nextLong();
//        this.sourceRef = controller.routeServer("source", 0L, "target", targetRef, null).get();
//
//        this.source = controller.supplySource("source", Source::new);
//        this.target = controller.supplyTarget("target", Target::new);
//
//        final long sourceId = random.nextLong();
//        final long acceptCorrelationId = random.nextLong();
//
//        source.reinit(sourceRef, sourceId, acceptCorrelationId);
//        target.reinit();
//
//        source.doBegin();
//    }
//
//    @TearDown(Level.Trial)
//    public void reset() throws Exception
//    {
//        SocksController controller = reaktor.controller(SocksController.class);
//
//        controller.unrouteServer("source", sourceRef, "target", targetRef, null).get();
//
//        this.source = null;
//        this.target = null;
//    }
//
//    @Benchmark
//    @Group("throughput")
//    @GroupThreads(1)
//    public void writer(Control control) throws Exception
//    {
//        while (!control.stopMeasurement &&
//               source.process() == 0)
//        {
//            Thread.yield();
//        }
//    }
//
//    @Benchmark
//    @Group("throughput")
//    @GroupThreads(1)
//    public void reader(Control control) throws Exception
//    {
//        while (!control.stopMeasurement &&
//               target.read() == 0)
//        {
//            Thread.yield();
//        }
//    }
//
//    private final class Source
//    {
//        private final MessagePredicate streams;
//        private final ToIntFunction<MessageConsumer> throttle;
//
//        private BeginFW begin;
//        private DataFW data;
//
//        private Source(
//            MessagePredicate streams,
//            ToIntFunction<MessageConsumer> throttle)
//        {
//            this.streams = streams;
//            this.throttle = throttle;
//        }
//
//        private void reinit(
//            long sourceRef,
//            long sourceId,
//            long acceptCorrelationId)
//        {
//            final MutableDirectBuffer writeBuffer = new UnsafeBuffer(new byte[256]);
//
//            final Consumer<ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> headers = hs ->
//            {
//                hs.item(h -> h.name(":scheme").value("http"));
//                hs.item(h -> h.name(":method").value("GET"));
//                hs.item(h -> h.name(":path").value("/"));
//                hs.item(h -> h.name("host").value("localhost:8080"));
//                hs.item(h -> h.name("upgrade").value("websocket"));
//                hs.item(h -> h.name("sec-websocket-key").value("dGhlIHNhbXBsZSBub25jZQ=="));
//                hs.item(h -> h.name("sec-websocket-version").value("13"));
//
////                hs.item(h -> h.name("sec-websocket-protocol").value(protocol));
//            };
//
//            // TODO: move to doBegin to avoid writeBuffer overwrite with DataFW
//            this.begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
//                    .streamId(sourceId)
//                    .sourceRef(sourceRef)
//                    .acceptCorrelationId(acceptCorrelationId)
//                    .extension(e -> e.set(visitHttpBeginEx(headers)))
//                    .build();
//
//            byte[] charBytes = "Hello, world".getBytes(StandardCharsets.UTF_8);
//
//            byte[] sendArray = new byte[18];
//            sendArray[0] = (byte) 0x82; // fin, binary
//            sendArray[1] = (byte) 0x8c; // masked, length 12
//            sendArray[2] = (byte) 0x01; // masking key (4 bytes)
//            sendArray[3] = (byte) 0x02;
//            sendArray[4] = (byte) 0x03;
//            sendArray[5] = (byte) 0x04;
//            sendArray[6] = (byte) (charBytes[0] ^ sendArray[2]);
//            sendArray[7] = (byte) (charBytes[1] ^ sendArray[3]);
//            sendArray[8] = (byte) (charBytes[2] ^ sendArray[4]);
//            sendArray[9] = (byte) (charBytes[3] ^ sendArray[5]);
//            sendArray[10] = (byte) (charBytes[4] ^ sendArray[2]);
//            sendArray[11] = (byte) (charBytes[5] ^ sendArray[3]);
//            sendArray[12] = (byte) (charBytes[6] ^ sendArray[4]);
//            sendArray[13] = (byte) (charBytes[7] ^ sendArray[5]);
//            sendArray[14] = (byte) (charBytes[8] ^ sendArray[2]);
//            sendArray[15] = (byte) (charBytes[9] ^ sendArray[3]);
//            sendArray[16] = (byte) (charBytes[10] ^ sendArray[4]);
//            sendArray[17] = (byte) (charBytes[11] ^ sendArray[5]);
//
//            this.data = dataRW.wrap(writeBuffer, this.begin.limit(), writeBuffer.capacity())
//                              .streamId(sourceId)
//                              .payload(p -> p.set(sendArray))
//                              .extension(e -> e.reset())
//                              .build();
//        }
//
//        private boolean doBegin()
//        {
//            return streams.test(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
//        }
//
//        private int process()
//        {
//            int work = 0;
//
//            if (streams.test(data.typeId(), data.buffer(), data.offset(), data.sizeof()))
//            {
//                work++;
//            }
//
//            work += throttle.applyAsInt((t, b, i, l) -> {});
//
//            return work;
//        }
//
//        private Flyweight.Builder.Visitor visitHttpBeginEx(
//            Consumer<ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> headers)
//        {
//            return (buffer, offset, limit) ->
//                httpBeginExRW.wrap(buffer, offset, limit)
//                             .headers(headers)
//                             .build()
//                             .sizeof();
//        }
//    }
//
//    private final class Target
//    {
//        private final ToIntFunction<MessageConsumer> streams;
//        private final MessagePredicate throttle;
//
//        private MutableDirectBuffer writeBuffer;
//        private MessageConsumer readHandler;
//
//        private Target(
//            ToIntFunction<MessageConsumer> streams,
//            MessagePredicate throttle)
//        {
//            this.streams = streams;
//            this.throttle = throttle;
//        }
//
//        private void reinit()
//        {
//            this.writeBuffer = new UnsafeBuffer(new byte[256]);
//            this.readHandler = this::beforeBegin;
//        }
//
//        private int read()
//        {
//            return streams.applyAsInt(readHandler);
//        }
//
//        private void beforeBegin(
//            int msgTypeId,
//            DirectBuffer buffer,
//            int index,
//            int length)
//        {
//            final BeginFW begin = beginRO.wrap(buffer, index, index + length);
//            final long streamId = begin.streamId();
//            doWindow(streamId, 8192);
//
//            this.readHandler = this::afterBegin;
//        }
//
//        private void afterBegin(
//            int msgTypeId,
//            DirectBuffer buffer,
//            int index,
//            int length)
//        {
//            final DataFW data = dataRO.wrap(buffer, index, index + length);
//            final long streamId = data.streamId();
//            final OctetsFW payload = data.payload();
//
//            final int update = payload.sizeof();
//            doWindow(streamId, update);
//        }
//
//        private boolean doWindow(
//            final long streamId,
//            final int update)
//        {
//            final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
//                    .streamId(streamId)
//                    .update(update)
//                    .build();
//
//            return throttle.test(window.typeId(), window.buffer(), window.offset(), window.sizeof());
//        }
//    }
//
//    public static void main(String[] args) throws RunnerException
//    {
//        Options opt = new OptionsBuilder()
//                .include(SocksServerBM.class.getSimpleName())
//                .forks(0)
//                .build();
//
//        new Runner(opt).run();
//    }
}
