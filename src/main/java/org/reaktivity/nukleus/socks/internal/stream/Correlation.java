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

public class Correlation
{

    // TODO comment these fields
    private long acceptCorrelationId;
    private String acceptName;
    private AcceptTransitionListener acceptTransitionListener;
    private long connectStreamId;
    private long connectRef;

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

    public String acceptName()
    {
        return acceptName;
    }

    public void acceptCorrelationId(long correlationId)
    {
        this.acceptCorrelationId = correlationId;
    }

    public void acceptName(String acceptName)
    {
        this.acceptName = acceptName;
    }

    public long connectStreamId()
    {
        return connectStreamId;
    }

    public void connectStreamId(long connectStreamId)
    {
        this.connectStreamId = connectStreamId;
    }

    public long connectRef()
    {
        return connectRef;
    }

    public void connectRef(long connectRef)
    {
        this.connectRef = connectRef;
    }
}
