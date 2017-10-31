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
package org.reaktivity.nukleus.socks.internal;

import org.reaktivity.nukleus.Configuration;

public class SocksConfiguration extends Configuration
{
    public static final String SOCKS_INITIAL_WINDOW = "nukleus.socks.initial.window";

    private static final int SOCKS_INITIAL_WINDOW_DEFAULT = 65536;

    public SocksConfiguration(
        Configuration config)
    {
        super(config);
    }

    public int socksInitialWindow()
    {
        return getInteger(SOCKS_INITIAL_WINDOW, SOCKS_INITIAL_WINDOW_DEFAULT);
    }

}
