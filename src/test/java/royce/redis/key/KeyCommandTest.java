package royce.redis.key;

import static org.assertj.core.api.SoftAssertions.assertSoftly;

import io.lettuce.core.KeyScanArgs.Builder;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import royce.redis.common.RedisConnectionProvider;

@SuppressWarnings("NonAsciiCharacters")
public class KeyCommandTest {

    @BeforeEach
    void cleanUp() {
        RedisCommands<String, String> syncConnection = RedisConnectionProvider.getSync();
        syncConnection.flushall();
    }

    @Test
    void KEY_타입과_관련_없이_존재_여부를_조회_할_수_있다() {
        // given
        var syncConnection = RedisConnectionProvider.getSync();
        syncConnection.hset("key", "field", "value");
        syncConnection.set("key2", "value");

        // when
        var existA = syncConnection.exists("key");
        var existB = syncConnection.exists("key2");
        var existC = syncConnection.exists("key3");

        // then
        assertSoftly(assertions -> {
            assertions.assertThat(existA).isEqualTo(1L);
            assertions.assertThat(existB).isEqualTo(1L);
            assertions.assertThat(existC).isEqualTo(0L);
        });
    }

    @Test
    void KEY_타입과_관련_없이_KEY를_지울_수_있다() {
        // given
        var syncConnection = RedisConnectionProvider.getSync();
        syncConnection.hset("key", "field", "value");
        syncConnection.set("key2", "value");

        // when
        var existA = syncConnection.del("key");
        var existB = syncConnection.del("key3");

        // then
        var exist = syncConnection.exists("key");
        assertSoftly(assertions -> {
            assertions.assertThat(exist).isEqualTo(0L);
            assertions.assertThat(existA).isEqualTo(1L);
            assertions.assertThat(existB).isEqualTo(0L);
        });
    }

    @Test
    void KEY_타입을_조회_할_수_있다() {
        // given
        var syncConnection = RedisConnectionProvider.getSync();
        syncConnection.hset("hashKey", "field", "value");
        syncConnection.pfadd("hyperloglogKey", "value");
        syncConnection.sadd("setKey", "value");
        syncConnection.zadd("sortedSetKey", 1, "value");

        // when
        var existA = syncConnection.type("hashKey");
        var existB = syncConnection.type("hyperloglogKey");
        var existC = syncConnection.type("setKey");
        var existD = syncConnection.type("sortedSetKey");

        // then
        assertSoftly(assertions -> {
            assertions.assertThat(existA).isEqualTo("hash");
            assertions.assertThat(existB).isEqualTo("string");
            assertions.assertThat(existC).isEqualTo("set");
            assertions.assertThat(existD).isEqualTo("zset");
        });
    }

    @Test
    void 해쉬_전체_조회_성능_평가() {
        // given
        var syncConnection = RedisConnectionProvider.getSync();
        Map<String, String> hashes = new LinkedHashMap<>();
        for (int i = 0; i < 100000; i++) {
            hashes.put("key" + i, "value" + i);
        }
        syncConnection.mset(hashes);

        // when
        Long hgetLatency = timeRater(() -> syncConnection.keys("*"));
        Long hsacnLatency = timeRater(() -> syncConnection.hscan("", Builder.matches("*")));

        System.out.println("hgetall latency: " + hgetLatency);
        System.out.println("hscan latency: " + hsacnLatency);
        // keys latency: 58
        // scan latency: 2
    }

    private Long timeRater(Supplier<?> supplier) {
        Instant now = Instant.now();
        supplier.get();
        Instant after = Instant.now();
        return Duration.between(now, after).toMillis();
    }
}
