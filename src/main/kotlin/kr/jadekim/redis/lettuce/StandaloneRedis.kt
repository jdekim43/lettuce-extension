package kr.jadekim.redis.lettuce

import io.lettuce.core.ConnectionFuture
import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import io.lettuce.core.support.BoundedPoolConfig
import kr.jadekim.redis.lettuce.codec.KeyPrefixStringCodec
import java.util.concurrent.CompletionStage

open class StandaloneRedis<K, V>(
        protected val uri: RedisURI,
        codec: RedisCodec<K, V>,
        poolSize: Int = BoundedPoolConfig.DEFAULT_MAX_TOTAL
) : Redis<K, V>(codec, poolSize) {

    companion object {

        fun stringCodec(
                uri: RedisURI,
                keyPrefix: String = "",
                poolSize: Int = BoundedPoolConfig.DEFAULT_MAX_TOTAL
        ) = StandaloneRedis(uri, KeyPrefixStringCodec(keyPrefix), poolSize)

        fun stringCodec(
                host: String,
                port: Int = 6379,
                dbIndex: Int = 0,
                password: String? = null,
                keyPrefix: String = "",
                poolSize: Int = BoundedPoolConfig.DEFAULT_MAX_TOTAL
        ) = stringCodec(RedisURI(host, port, dbIndex, password), keyPrefix, poolSize)
    }

    constructor(
            host: String,
            port: Int = 6379,
            dbIndex: Int = 0,
            password: String? = null,
            codec: RedisCodec<K, V>,
            poolSize: Int = BoundedPoolConfig.DEFAULT_MAX_TOTAL
    ) : this(RedisURI(host, port, dbIndex, password), codec, poolSize)

    override fun connectForExecute(): ConnectionFuture<StatefulRedisConnection<K, V>> {
        return client.connectAsync(codec, uri)
    }

    override fun connectPubSub(): CompletionStage<StatefulRedisPubSubConnection<K, V>> {
        return client.connectPubSubAsync(codec, uri)
    }

    override fun connectForRead(): CompletionStage<StatefulRedisConnection<K, V>> {
        return client.connectAsync(codec, uri)
    }
}