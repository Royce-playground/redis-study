package royce.redis.datatype;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.lettuce.core.KeyValue;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import royce.redis.common.RedisConnectionProvider;

@SuppressWarnings("NonAsciiCharacters")
class StringTypeTest {

    @BeforeEach
    void cleanUp() {
        RedisCommands<String, String> syncConnection = RedisConnectionProvider.getSync();
        syncConnection.flushall();
    }

    /**
     * Redis String
     * Redis 문자열은 텍스트, 직렬화된 개체, 바이너리 배열을 포함한 바이트 시퀀스(jpeg 같은 파일 포함) 저장 한다.
     * 문자열은 캐싱 뿐만 아니라 카운터를 구현하고 비트 단위 연산을 수행할 수 있는 기능도 지원. 최대 크기는 512MB이다.
     * GET과 SET을 통해 문자열 값을 저장, 조회 한다.
     * 이때, 이미 있는 키를 SET 하면 대체 된다. 추가로, 다른 인자나 명령들을 활용하여 중복된 키에 대한 정책을 정할 수 있다.
     *
     */
    @Test
    void 간단한_저장_및_조회() {
        // given
        RedisCommands<String, String> syncConnection = RedisConnectionProvider.getSync();
        syncConnection.set("key", "value");

        // when
        String value = syncConnection.get("key");

        // then
        assertThat(value).isEqualTo("value");
    }

    @Test
    void 키가_이미_존재_하는_경우_값은_대체_된다() {
        // given
        RedisCommands<String, String> syncConnection = RedisConnectionProvider.getSync();
        syncConnection.set("key", "value");
        syncConnection.set("key", "newValue");

        // when
        String value = syncConnection.get("key");

        // then
        assertThat(value).isEqualTo("newValue");
    }

    @Test
    void SETNX는_키가_이미_존재_하면_저장_하지_않는다() {
        // given
        RedisCommands<String, String> syncConnection = RedisConnectionProvider.getSync();
        syncConnection.set("key", "value");

        // when
        // deprecated
        // syncConnection.setnx("key", "newValue");
        syncConnection.set("key", "newValue", SetArgs.Builder.nx());

        // then
        String value = syncConnection.get("key");
        assertThat(value).isEqualTo("value");
    }

    @Test
    void SETXX는_키가_이미_존재_하는_경우에만_저장_한다() {
        // given
        RedisCommands<String, String> syncConnection = RedisConnectionProvider.getSync();

        // when
        syncConnection.set("key", "value", SetArgs.Builder.xx());

        // then
        String value = syncConnection.get("key");
        assertThat(value).isNull();
    }

    @Test
    void 단일_명령으로_여러_데이터를_저장_조회_한다() {
        // given
        RedisCommands<String, String> syncConnection = RedisConnectionProvider.getSync();

        // when
        syncConnection.mset(Map.of(
                "key", "value",
                "key2", "value2",
                "key3", "value3"
        ));

        // then
        List<KeyValue<String, String>> mget = syncConnection.mget("key", "key2", "key3");
        assertThat(mget).hasSize(3);
        List<String> values = mget.stream().map(KeyValue::getValue).toList();
        assertThat(values).containsExactly("value", "value2", "value3");
    }

    @Test
    void MSET시_key가_중복_되면_예외가_발생_한다() {
        // given
        RedisCommands<String, String> syncConnection = RedisConnectionProvider.getSync();

        // when & then
        assertThatThrownBy(() -> syncConnection.mset(Map.of(
                "key", "value",
                "key", "value2"
        ))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void 숫자로_저장된_값에_대하여_증감_연산이_가능_하다() {
        // given
        RedisCommands<String, String> syncConnection = RedisConnectionProvider.getSync();
        syncConnection.set("user:royce:score", "80");

        // when & then
        syncConnection.incr("user:royce:score");
        assertThat(syncConnection.get("user:royce:score")).isEqualTo("81");

        syncConnection.incrby("user:royce:score", 20);
        assertThat(syncConnection.get("user:royce:score")).isEqualTo("101");

        syncConnection.decr("user:royce:score");
        assertThat(syncConnection.get("user:royce:score")).isEqualTo("100");

        syncConnection.decrby("user:royce:score", 20);
        assertThat(syncConnection.get("user:royce:score")).isEqualTo("80");

        syncConnection.incrbyfloat("user:royce:score", 0.5);
        assertThat(syncConnection.get("user:royce:score")).isEqualTo("80.5");
    }

    @Nested
    class AtomicIncrementTest {

        static class RedisIncr implements Callable<Void> {

            @Override
            public Void call() {
                for (int i = 0; i < 10000; i++) {
                    RedisCommands<String, String> syncConnection = RedisConnectionProvider.getSync();
                    syncConnection.incr("test");
                }

                return null;
            }
        }

        @Test
        void 증감_연산은_원자적_연산을_보장_한다() throws InterruptedException {
            // given
            ExecutorService executorService = Executors.newFixedThreadPool(10);
            List<RedisIncr> runners = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                runners.add(new RedisIncr());
            }

            // when
            executorService.invokeAll(runners);

            // then
            RedisCommands<String, String> syncConnection = RedisConnectionProvider.getSync();
            assertThat(syncConnection.get("test")).isEqualTo("100000");
        }
    }
}


