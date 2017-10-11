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

import java.util.function.Consumer;

import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.socks.internal.types.control.RouteFW;

public class Correlation
{

    // Can be used to send RESET and WINDOW back to the SOURCE on the ACCEPT stream
    private MessageConsumer acceptThrottle;

    // ACCEPT stream identifier
    private long acceptStreamId;

    // ACCEPT SOURCE reference (similar to port)
    private long acceptSourceRef;

    // ACCEPT SOURCE name (similar to address)
    private String acceptSourceName;

    // Can be used to send BEGIN, DATA, END, ABORT frames to the SOURCE on the ACCEPT-REPLY stream
    private MessageConsumer acceptReplyEndpoint;

    // ACCEPT-REPLY stream identifier
    private long acceptReplyStreamId;

    // Used to identify the Correlation on the ACCEPT/ACCEPT-RELPY streams
    private long acceptCorrelationId;

    // Can be used to send BEGIN, DATA, END, ABORT frames to the TARGET on the CONNECT stream
    private MessageConsumer connectEndpoint;

    // CONNECT TARGET name (similar to address)
    private String connectTargetName;

    // CONNECT TARGET reference (similar to port)
    private long connectTargetRef;

    // Used to identify the Correlation on the CONNECT/CONNECT-RELPY streams
    private long connectCorrelationId;

    // CONNECT stream identifier
    private long connectStreamId;

    private AcceptTransitionListener acceptTransitionListener;

    private RouteFW connectRoute;

    private Consumer<Boolean> nextAcceptSignal;

    private MessageConsumer connectReplyThrottle;

    private long connectReplyStreamId;

    public AcceptTransitionListener acceptTransitionListener()
    {
        return acceptTransitionListener;
    }

    public void acceptTransitionListener(AcceptTransitionListener acceptTransitionListener)
    {
        this.acceptTransitionListener = acceptTransitionListener;
    }

    public long acceptCorrelationId()
    {
        return acceptCorrelationId;
    }

    public void acceptCorrelationId(long correlationId)
    {
        this.acceptCorrelationId = correlationId;
    }

    public String acceptSourceName()
    {
        return acceptSourceName;
    }

    public void acceptSourceName(String acceptSourceName)
    {
        this.acceptSourceName = acceptSourceName;
    }

    public long connectStreamId()
    {
        return connectStreamId;
    }

    public void connectStreamId(long connectStreamId)
    {
        this.connectStreamId = connectStreamId;
    }

    public long connectTargetRef()
    {
        return connectTargetRef;
    }

    public void connectTargetRef(long connectTargetRef)
    {
        this.connectTargetRef = connectTargetRef;
    }

    public MessageConsumer connectEndpoint()
    {
        return connectEndpoint;
    }

    public void connectEndpoint(MessageConsumer connectEndpoint)
    {
        this.connectEndpoint = connectEndpoint;
    }

    public MessageConsumer acceptThrottle()
    {
        return acceptThrottle;
    }

    public void acceptThrottle(MessageConsumer acceptThrottle)
    {
        this.acceptThrottle = acceptThrottle;
    }

    public long acceptStreamId()
    {
        return acceptStreamId;
    }

    public void acceptStreamId(long acceptStreamId)
    {
        this.acceptStreamId = acceptStreamId;
    }

    public long acceptSourceRef()
    {
        return acceptSourceRef;
    }

    public void acceptSourceRef(long acceptSourceRef)
    {
        this.acceptSourceRef = acceptSourceRef;
    }

    public MessageConsumer acceptReplyEndpoint()
    {
        return acceptReplyEndpoint;
    }

    public void acceptReplyEndpoint(MessageConsumer acceptReplyEndpoint)
    {
        this.acceptReplyEndpoint = acceptReplyEndpoint;
    }

    public long acceptReplyStreamId()
    {
        return acceptReplyStreamId;
    }

    public void acceptReplyStreamId(long acceptReplyStreamId)
    {
        this.acceptReplyStreamId = acceptReplyStreamId;
    }

    public String connectTargetName()
    {
        return connectTargetName;
    }

    public void connectTargetName(String connectTargetName)
    {
        this.connectTargetName = connectTargetName;
    }

    public long connectCorrelationId()
    {
        return connectCorrelationId;
    }

    public void connectCorrelationId(long connectCorrelationId)
    {
        this.connectCorrelationId = connectCorrelationId;
    }

    public RouteFW connectRoute()
    {
        return connectRoute;
    }

    public void connectRoute(RouteFW connectRoute)
    {
        this.connectRoute = connectRoute;
    }

    public Consumer<Boolean> nextAcceptSignal()
    {
        return nextAcceptSignal;
    }

    public void nextAcceptSignal(Consumer<Boolean> nextAcceptSignal)
    {
        this.nextAcceptSignal = nextAcceptSignal;
    }

    public MessageConsumer connectReplyThrottle()
    {
        return connectReplyThrottle;
    }

    public void connectReplyThrottle(MessageConsumer connectReplyThrottle)
    {
        this.connectReplyThrottle = connectReplyThrottle;
    }

    public long connectReplyStreamId()
    {
        return connectReplyStreamId;
    }

    public void connectReplyStreamId(long connectReplyId)
    {
        this.connectReplyStreamId = connectReplyId;
    }
}
