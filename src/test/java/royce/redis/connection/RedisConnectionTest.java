package royce.redis.connection;

import static org.assertj.core.api.Assertions.assertThat;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.RedisURI.Builder;
import io.lettuce.core.api.StatefulRedisConnection;
import org.junit.jupiter.api.Test;

class RedisConnectionTest {

    private static final String HOST = "localhost";
    private static final int PORT = 6379;
    private static final String PASSWORD = "royceredis";


    @Test
    void connection() {
        RedisURI redisURI = Builder.redis(HOST)
                .withPort(PORT)
                .withPassword(PASSWORD.toCharArray())
                .withDatabase(0)
                .build();
        RedisClient client = RedisClient.create(redisURI);

        StatefulRedisConnection<String, String> connect = client.connect();
        String pong = connect.sync().ping();

        assertThat(pong).isEqualTo("PONG");
    }
}
