package tair;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.SocketOptions.KeepAliveOptions;
import io.lettuce.core.SocketOptions.TcpUserTimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.protocol.ProtocolVersion;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class Main {

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    public static void main(String[] args) throws InterruptedException {
        String host = "r-2zea1t9rbffzdm8nai.redis.rds.aliyuncs.com";
        int port = 6379;

        RedisURI redisURI = RedisURI.Builder.redis(host).withPort(port).build();

        ClusterTopologyRefreshOptions refreshOptions = ClusterTopologyRefreshOptions.builder()
                .enablePeriodicRefresh(Duration.ofSeconds(3000000)).dynamicRefreshSources(true)
                .enableAllAdaptiveRefreshTriggers().adaptiveRefreshTriggersTimeout(Duration.ofSeconds(20)).build();

        RedisClusterClient redisClient = RedisClusterClient.create(redisURI);
        redisClient.setOptions(ClusterClientOptions.builder().protocolVersion(ProtocolVersion.RESP2)
                .validateClusterNodeMembership(false).topologyRefreshOptions(refreshOptions).build());

        // GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool = ConnectionPoolSupport
        //         .createGenericObjectPool(() -> redisClient.connect(), new GenericObjectPoolConfig());
        StatefulRedisClusterConnection<String, String> connection = redisClient.connect();
        for (int i = 0; i < 1000000; i++) {
            try {
                // StatefulRedisClusterConnection<String, String> connection = pool.borrowObject();
                long begin = System.currentTimeMillis();
                for (int j = 0; j < 5000; j++) {
                    connection.sync().set("" + i + j, "" + i + j);
                    connection.sync().get("" + i + j);
                }
                long end = System.currentTimeMillis();
                String now = LocalDateTime.now().format(formatter);
                System.out.println("now: " + now + ",i: " + i + ", cost: " + (end - begin) + "ms");
                // pool.returnObject(connection);
            } catch (Exception e) {
                System.out.println("\nException throwed!\n");
                e.printStackTrace();
            }
        }

    }

}
