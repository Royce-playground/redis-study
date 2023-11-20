package royce.redis.datatype;

import static org.assertj.core.api.Assertions.assertThat;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import royce.redis.common.RedisConnectionProvider;

@SuppressWarnings("NonAsciiCharacters")
public class ListsTest {

    @BeforeEach
    void cleanUp() {
        RedisCommands<String, String> syncConnection = RedisConnectionProvider.getSync();
        syncConnection.flushall();
    }

    @Test
    void 리스트_생성() {
        // given
        var syncConnection = RedisConnectionProvider.getSync();
        syncConnection.lpush("playlists", "playlist:1");
        assertThat(syncConnection.type("playlists")).isEqualTo("list");

        // when
        var value = syncConnection.lrange("playlists", 0, -1).get(0);

        // then
        assertThat(value).isEqualTo("playlist:1");
    }

    @Test
    void 리스트는_순서를_보장_하며_중복된_값을_저장_한다() {
        // given
        var syncConnection = RedisConnectionProvider.getSync();
        syncConnection.lpush("playlists", "playlist:1");

        // when
        syncConnection.rpush("playlists", "playlist:2");
        syncConnection.lpush("playlists", "playlist:0");
        syncConnection.rpush("playlists", "playlist:2");

        // then
        var all = syncConnection.lrange("playlists", 0, -1);
        assertThat(all).containsExactly(
                "playlist:0", "playlist:1", "playlist:2", "playlist:2"
        );
    }

    @Test
    void 리스트_요소_제거() {
        var syncConnection = RedisConnectionProvider.getSync();
        syncConnection.rpush("playlists", "playlist:1");
        syncConnection.rpush("playlists", "playlist:2");
        syncConnection.rpush("playlists", "playlist:0");
        syncConnection.rpush("playlists", "playlist:2");
        syncConnection.rpush("playlists", "playlist:1");
        syncConnection.rpush("playlists", "playlist:2");
        assertThat(syncConnection.lrange("playlists", 0, -1)).containsExactly(
                "playlist:1", "playlist:2", "playlist:0", "playlist:2", "playlist:1", "playlist:2"
        );

        assertThat(syncConnection.lpop("playlists")).isEqualTo("playlist:1");
        assertThat(syncConnection.lpop("playlists")).isEqualTo("playlist:2");
        assertThat(syncConnection.lrange("playlists", 0, -1)).containsExactly(
                "playlist:0", "playlist:2", "playlist:1", "playlist:2"
        );

        syncConnection.ltrim("playlists", 0, 1);
        assertThat(syncConnection.lrange("playlists", 0, -1)).containsExactly(
                "playlist:0", "playlist:2"
        );
    }

    @Test
    void 블로킹_연산() {
        // given
        RedisClient clientA = RedisConnectionProvider.client();
        var counterConnection = clientA.connect().sync();
        int connectionCountBeforeBlocking = RedisConnectionProvider.getConnectionCount(counterConnection);

        RedisClient clientB = RedisConnectionProvider.client();
        var syncConnection = clientB.connect().async();
        syncConnection.brpop(10, "playlists");

        // when
        int connectionCountAfterBlocking = RedisConnectionProvider.getConnectionCount(counterConnection);
        assertThat(connectionCountAfterBlocking).isEqualTo(connectionCountBeforeBlocking + 1);

        // then
        // insert element by other connection
        var otherConnection = RedisConnectionProvider.getSync();
        otherConnection.rpush("playlists", "playlist:1");
        assertThat(otherConnection.lrange("playlists", 0, -1)).isEmpty();
    }
}
