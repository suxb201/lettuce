package tair;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.SocketOptions.KeepAliveOptions;
import io.lettuce.core.SocketOptions.TcpUserTimeoutOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.protocol.ProtocolVersion;

public class Main {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    public static void main(String[] args) throws InterruptedException {
        String host = "127.0.0.1";
        int port = 30001;
        String password = "foobar";

        RedisURI redisURI = RedisURI.Builder.redis(host).withPort(port).withPassword(password.toCharArray()).build();

        ClusterTopologyRefreshOptions refreshOptions = ClusterTopologyRefreshOptions.builder()
                .enablePeriodicRefresh(Duration.ofSeconds(3000000))
                .dynamicRefreshSources(true)
                .enableAllAdaptiveRefreshTriggers()
                .adaptiveRefreshTriggersTimeout(Duration.ofSeconds(1)).build();

        RedisClusterClient redisClient = RedisClusterClient.create(redisURI);
        redisClient.setOptions(ClusterClientOptions.builder()
                .protocolVersion(ProtocolVersion.RESP2)
                .validateClusterNodeMembership(false)
                .topologyRefreshOptions(refreshOptions).build());

        StatefulRedisClusterConnection<String, String> connection = redisClient.connect();
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            try {
                long begin = System.currentTimeMillis();
                for (int j = 0; j < 10000; j++) {
                    connection.sync().set("" + i + j, "" + i + j);
                    connection.sync().get("" + i + j);
                }
                long end = System.currentTimeMillis();
                String now = LocalDateTime.now().format(formatter);
                System.out.println("now: " + now + ",i: " + i + ", cost: " + (end - begin) + "ms");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
}