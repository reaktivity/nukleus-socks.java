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
package org.reaktivity.nukleus.socks.internal.stream.types;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.socks.internal.types.Flyweight;

public abstract class FragmentedFlyweight<T extends FragmentedFlyweight> extends Flyweight
{
    public enum ReadState
    {
        FULL,
        INCOMPLETE,
        BROKEN
    }

    public enum BuildState
    {
        INITIAL,
        FINAL,
        BROKEN
    }

    public T wrap(
        DirectBuffer buffer,
        int offset,
        int maxLimit)
    {
        super.wrap(buffer, offset, maxLimit);
        checkLimit(limit(), maxLimit);
        return (T) this;
    }

    public abstract ReadState canWrap(
        DirectBuffer buffer,
        int offset,
        int maxLimit);

    protected abstract BuildState getBuildState();
}
