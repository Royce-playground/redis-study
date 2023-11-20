package royce.redis.datatype;

import static org.assertj.core.api.Assertions.assertThat;

import io.lettuce.core.ScanArgs.Builder;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import royce.redis.common.RedisConnectionProvider;

@SuppressWarnings("NonAsciiCharacters")
public class SetsTest {

    @BeforeEach
    void cleanUp() {
        RedisCommands<String, String> syncConnection = RedisConnectionProvider.getSync();
        syncConnection.flushall();
    }

    @Test
    void Set_생성() {
        // given
        var syncConnection = RedisConnectionProvider.getSync();
        syncConnection.sadd("user:online", "user:1", "user:2", "user:3");

        // when
        List<String> values = syncConnection.sscan("user:online", Builder.matches("user:*")).getValues();

        // then
        assertThat(values).hasSize(3);
        assertThat(values).containsExactlyInAnyOrder("user:1", "user:2", "user:3");
    }

    @Test
    void Set_연산() {
        // given
        var syncConnection = RedisConnectionProvider.getSync();
        syncConnection.sadd("user:online", "user:1", "user:2", "user:3");
        syncConnection.sadd("user:online:redis-study", "user:2", "user:3");

        // when
        Set<String> onlineUserInRedisStudy = syncConnection.sinter("user:online", "user:online:redis-study");

        // then
        assertThat(onlineUserInRedisStudy).hasSize(2);
        assertThat(onlineUserInRedisStudy).containsExactlyInAnyOrder("user:2", "user:3");
    }

    @Test
    void Set_제거() {
        // given
        var syncConnection = RedisConnectionProvider.getSync();
        syncConnection.sadd("user:online",
                "user:1", "user:2", "user:3", "temp:user:1", "temp:user:2", "temp:user:3"
        );


        // when
        syncConnection.srem("user:online",
                "temp:user:1", "temp:user:2", "temp:user:3"
        );

        // then
        Long sizeOfSet = syncConnection.scard("user:online");
        assertThat(sizeOfSet).isEqualTo(3);
    }

    @Test
    void Set_랜덤으로_제거() {
        // given
        var syncConnection = RedisConnectionProvider.getSync();
        syncConnection.sadd("user:online",
                "user:1", "user:2", "user:3", "temp:user:1", "temp:user:2", "temp:user:3"
        );


        // when
        syncConnection.spop("user:online", 2);

        // then
        Long sizeOfSet = syncConnection.scard("user:online");
        assertThat(sizeOfSet).isEqualTo(4);
    }
}


