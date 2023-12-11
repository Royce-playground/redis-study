package royce.redis.common;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.RedisURI.Builder;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.Arrays;

public class RedisConnectionProvider {

    private static final String HOST = "localhost";
    private static final int PORT = 6379;
    private static final String PASSWORD = "royceredis";
    private static final StatefulRedisConnection<String, String> redisConnection = connection();

    private RedisConnectionProvider() {
    }

    private static StatefulRedisConnection<String, String> connection() {
        return client().connect();
    }

    public static RedisClient client() {
        RedisURI redisURI = Builder.redis(HOST)
                .withPort(PORT)
                .withPassword(PASSWORD.toCharArray())
                .withDatabase(0)
                .build();
        return RedisClient.create(redisURI);
    }

    public static RedisCommands<String, String> getSync() {
        return redisConnection.sync();
    }

    public static int getConnectionCount(RedisCommands<String, String> syncConnection) {
        String infos = syncConnection.info();
        String connectedClientsCount = Arrays.stream(infos.split(System.lineSeparator()))
                .filter(line -> line.startsWith("connected_clients"))
                .findAny().orElseThrow(); // result -> connected_clients:1

        System.out.println(connectedClientsCount);
        String connectedClientCount = connectedClientsCount.strip().split(":")[1];
        return Integer.parseInt(connectedClientCount);
    }
}
