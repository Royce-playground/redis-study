package royce.redis.datatype;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.ScanArgs.Builder;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import royce.redis.common.RedisConnectionProvider;

@SuppressWarnings("NonAsciiCharacters")
public class HashesTest {

    @BeforeEach
    void cleanUp() {
        RedisCommands<String, String> syncConnection = RedisConnectionProvider.getSync();
        syncConnection.flushall();
    }

    @Test
    void 해쉬_생성() {
        // given
        RedisCommands<String, String> syncConnection = RedisConnectionProvider.getSync();
        syncConnection.hset("endpoint:/admin/{id}", "caller", "user:123");

        // when
        String value = syncConnection.hget(":endpoint:/admin/{id}", "caller");

        // then
        assertThat(value).isEqualTo("user:123");
    }

    @Test
    void 해쉬_수정() {
        // given
        RedisCommands<String, String> syncConnection = RedisConnectionProvider.getSync();
        String endpointKey = "endpoint:/admin/{id}";
        syncConnection.hset(endpointKey, Map.of(
                "caller", "user:123",
                "initAccessIp", "127.0.0.1"
                )
        ); //

        // when
        syncConnection.hset(endpointKey, "caller", "user:456");
        syncConnection.hset(endpointKey, "lastAccessTime", Instant.now().toString());
        syncConnection.hsetnx(endpointKey, "initAccessIp", "192.1.1.1");
        syncConnection.hincrby(endpointKey, "count", 1);

        // then
        String caller = syncConnection.hget(endpointKey, "caller");
        String count = syncConnection.hget(endpointKey, "count");
        String lastAccessTime = syncConnection.hget(endpointKey, "lastAccessTime");
        String initAccessIp = syncConnection.hget(endpointKey, "initAccessIp");
        assertThat(caller).isEqualTo("user:456");
        assertThat(count).isEqualTo("1");
        assertThat(lastAccessTime).isNotNull();
        assertThat(initAccessIp).isNotEqualTo("192.1.1.1");
    }

    @Test
    void 해쉬_여러_필드_조회() {
        // given
        RedisCommands<String, String> syncConnection = RedisConnectionProvider.getSync();
        String endpointKey = "endpoint:/admin/{id}";
        syncConnection.hset(endpointKey, Map.of(
                        "caller", "user:123",
                        "initAccessIp", "127.0.0.1"
                )
        );

        // when
        List<KeyValue<String, String>> result = syncConnection.hmget(endpointKey,
                "caller", "initAccessIp", "lastAccessTime", "count"
        );

        // then
        assertThat(result).hasSize(4);
        assertThat(result.get(0).getValue()).isEqualTo("user:123");
        assertThat(result.get(1).getValue()).isEqualTo("127.0.0.1");

        assertThat(result.get(2).isEmpty()).isTrue();
        assertThat(result.get(3).isEmpty()).isTrue();
    }

    @Test
    void 다른_데이터_타입으로_필드_조회() {
        // given
        RedisCommands<String, String> syncConnection = RedisConnectionProvider.getSync();
        String endpointKey = "endpoint:/admin/{id}";
        syncConnection.hset(endpointKey, Map.of(
                        "caller", "user:123",
                        "initAccessIp", "127.0.0.1"
                )
        );

        // when
        assertThatThrownBy(() -> syncConnection.get(endpointKey))
                .isInstanceOf(RedisCommandExecutionException.class)
                .hasMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    @Test
    void 해쉬_전체_조회_성능_평가() {
        // given
        RedisCommands<String, String> syncConnection = RedisConnectionProvider.getSync();
        String endpointKey = "endpoint:/admin/{id}";
        Map<String, String> hashes = new LinkedHashMap<>();
        for (int i = 0; i < 100000; i++) {
            hashes.put("key" + i, "value" + i);
        }
        syncConnection.hset(endpointKey, hashes);

        // when
        Long hgetLatency = timeRater(() -> syncConnection.hgetall(endpointKey));
        Long hsacnLatency = timeRater(() -> syncConnection.hscan(endpointKey, Builder.matches("*")));

        System.out.println("hgetall latency: " + hgetLatency);
        System.out.println("hscan latency: " + hsacnLatency);
        // hgetall latency: 107
        // hscan latency: 2
    }

    private Long timeRater(Supplier<?> supplier) {
        Instant now = Instant.now();
        supplier.get();
        Instant after = Instant.now();
        return Duration.between(now, after).toMillis();
    }
}
