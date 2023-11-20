package royce.redis.datatype;

import static org.assertj.core.api.Assertions.assertThat;

import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import royce.redis.common.RedisConnectionProvider;

@SuppressWarnings("NonAsciiCharacters")
public class HLLTest {

    @BeforeEach
    void cleanUp() {
        RedisCommands<String, String> syncConnection = RedisConnectionProvider.getSync();
        syncConnection.flushall();
    }

    @Test
    void HLL_생성_테스트() {
        // given
        var syncConnection = RedisConnectionProvider.getSync();
        syncConnection.pfadd("endpoint:/admin", "user:1", "user:2", "user:3");

        // when
        long count = syncConnection.pfcount("endpoint:/admin");

        // then
        assertThat(count).isEqualTo(3);
    }

    @Test
    void HLL_MERGE_테스트() {
        // given
        var syncConnection = RedisConnectionProvider.getSync();
        syncConnection.pfadd("endpoint:/admin/move", "user:1", "user:2", "user:3", "user:4");
        syncConnection.pfadd("endpoint:/admin/enroll", "user:1", "user:2");
        syncConnection.pfadd("endpoint:/admin/remove", "user:3", "user:4", "user:5");

        // when
        syncConnection.pfmerge("endpoint:/admin", "endpoint:/admin/enroll", "endpoint:/admin/remove");

        // then
        long count = syncConnection.pfcount("endpoint:/admin");
        assertThat(count).isEqualTo(5);
    }

    @Test
    void SET과_성능_비교() {
        // given
        var syncConnection = RedisConnectionProvider.getSync();

        // when
        Long setTime = timeRater(() -> {
            for (int i = 0; i < 10000; i++) {
                syncConnection.sadd("user:online:set", "user:" + i);
            }
            return syncConnection.scard("user:online");
        });

        Long hllTime = timeRater(() -> {
            for (int i = 0; i < 10000; i++) {
                syncConnection.pfadd("user:online:hll", "user:" + i);
            }
            return syncConnection.pfcount("user:online:hll");
        });

        // then
        Long setMemory = syncConnection.memoryUsage("user:online:set");
        Long hllMemory = syncConnection.memoryUsage("user:online:hll");
        System.out.println("setTime = " + setTime + ", setMemory = " + setMemory);
        System.out.println("hllTime = " + hllTime + ", hllMemory = " + hllMemory);
//        setTime = 6557, setMemory = 596728
//        hllTime = 6476, hllMemory = 14400
    }

    private Long timeRater(Supplier<?> supplier) {
        Instant now = Instant.now();
        supplier.get();
        Instant after = Instant.now();
        return Duration.between(now, after).toMillis();
    }
}
