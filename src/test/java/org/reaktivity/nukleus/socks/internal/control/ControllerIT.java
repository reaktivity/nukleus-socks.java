/**
 * Copyright 2016-2019 The Reaktivity Project
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
package org.reaktivity.nukleus.socks.internal.control;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
//import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;
import org.reaktivity.nukleus.socks.internal.SocksController;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.reaktivity.nukleus.route.RouteKind.SERVER;

public class ControllerIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/socks/control/route")
        .addScriptRoot("unroute", "org/reaktivity/specification/nukleus/socks/control/unroute");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(4096)
        .controller("socks"::equals);

    private final Gson gson = new Gson();

    @Test
    @Specification({
        "${route}/server/nukleus"
    })
    public void shouldRouteServer() throws Exception
    {
        k3po.start();

        reaktor.controller(SocksController.class)
                .route(SERVER, "socks#0", "target#0")
                .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${routeExt}/server/nukleus"
    })
    public void shouldRouteServerWithExt() throws Exception
    {
        k3po.start();

        final JsonObject extension = new JsonObject();
        extension.addProperty("topic", "sensor/one");

        reaktor.controller(SocksController.class)
                .route(SERVER, "socks#0", "target#0", gson.toJson(extension))
                .get();

        k3po.finish();
    }

}
