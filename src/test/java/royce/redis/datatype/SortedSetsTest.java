package royce.redis.datatype;

import static org.assertj.core.api.Assertions.assertThat;

import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import royce.redis.common.RedisConnectionProvider;

@SuppressWarnings("NonAsciiCharacters")
public class SortedSetsTest {

    @BeforeEach
    void cleanUp() {
        RedisCommands<String, String> syncConnection = RedisConnectionProvider.getSync();
        syncConnection.flushall();
    }

    @Test
    void SortedSet_생성() {
        // given
        var syncConnection = RedisConnectionProvider.getSync();

        // when
        syncConnection.zadd("user:rank", 5, "user:1");
        syncConnection.zadd("user:rank", 4, "user:2");
        syncConnection.zadd("user:rank", 3, "user:3");

        // then
        var ranks = syncConnection.zrange("user:rank", 0, -1);
        assertThat(ranks).containsExactly(
                "user:3", "user:2", "user:1"
        );

        Long rank = syncConnection.zrank("user:rank", "user:1");
        assertThat(rank).isEqualTo(2L);

        Long reverseRank = syncConnection.zrevrank("user:rank", "user:1");
        assertThat(reverseRank).isEqualTo(0L);

        Double score = syncConnection.zscore("user:rank", "user:1");
        assertThat(score).isEqualTo(5.0);

        syncConnection.zremrangebyrank("user:rank", 0, 1);
        ranks = syncConnection.zrange("user:rank", 0, -1);
        assertThat(ranks).containsExactly("user:1");
    }
}


