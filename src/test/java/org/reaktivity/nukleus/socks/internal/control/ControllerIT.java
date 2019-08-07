package org.reaktivity.nukleus.socks.internal.control;

import com.google.gson.Gson;
import org.junit.Rule;
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

public class ControllerIT {
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

    @Rule
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
                .route(SERVER, "socks#0", "socks#0")
                .get();

        k3po.finish();
    }


}
