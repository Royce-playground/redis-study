package royce.redis.datatype;

import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import royce.redis.common.RedisConnectionProvider;

@SuppressWarnings("NonAsciiCharacters")
public class StreamTest {

    @BeforeEach
    void cleanUp() {
        RedisCommands<String, String> syncConnection = RedisConnectionProvider.getSync();
        syncConnection.flushall();
    }

    @Test
    void Stream_생성() {
        // given
        var syncConnection = RedisConnectionProvider.getSync();
        syncConnection.xadd("user:stream", "user:1", "royce", "user:2", "royce");
    }
}


