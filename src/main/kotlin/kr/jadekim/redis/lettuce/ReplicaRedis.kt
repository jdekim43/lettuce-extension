package kr.jadekim.redis.lettuce

import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.masterreplica.MasterReplica
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import io.lettuce.core.support.BoundedPoolConfig
import kr.jadekim.redis.lettuce.codec.KeyPrefixStringCodec
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

open class ReplicaRedis<K, V>(
        protected val masterUri: RedisURI,
        codec: RedisCodec<K, V>,
        executePoolSize: Int = BoundedPoolConfig.DEFAULT_MAX_TOTAL,
        readPoolSize: Int = BoundedPoolConfig.DEFAULT_MAX_TOTAL
) : Redis<K, V>(codec, executePoolSize, readPoolSize) {

    companion object {

        fun stringCodec(
                masterUri: RedisURI,
                keyPrefix: String = "",
                executePoolSize: Int = BoundedPoolConfig.DEFAULT_MAX_TOTAL,
                readPoolSize: Int = BoundedPoolConfig.DEFAULT_MAX_TOTAL
        ) = ReplicaRedis(masterUri, KeyPrefixStringCodec(keyPrefix), executePoolSize, readPoolSize)

        fun stringCodec(
                host: String,
                port: Int = 6379,
                dbIndex: Int = 0,
                password: String? = null,
                keyPrefix: String = "",
                executePoolSize: Int = BoundedPoolConfig.DEFAULT_MAX_TOTAL,
                readPoolSize: Int = BoundedPoolConfig.DEFAULT_MAX_TOTAL
        ) = stringCodec(RedisURI(host, port, dbIndex, password), keyPrefix, executePoolSize, readPoolSize)
    }

    private val nodes = mutableListOf(masterUri)

    constructor(
            host: String,
            port: Int = 6379,
            dbIndex: Int = 0,
            password: String? = null,
            codec: RedisCodec<K, V>,
            executePoolSize: Int = BoundedPoolConfig.DEFAULT_MAX_TOTAL,
            readPoolSize: Int = BoundedPoolConfig.DEFAULT_MAX_TOTAL
    ) : this(RedisURI(host, port, dbIndex, password), codec, executePoolSize, readPoolSize)

    fun replica(uri: RedisURI): ReplicaRedis<K, V> {
        nodes.add(uri)

        return this
    }

    fun replica(
            host: String,
            port: Int = 6379,
            dbIndex: Int = 0,
            password: String? = null
    ): ReplicaRedis<K, V> = replica(RedisURI(host, port, dbIndex, password))

    operator fun plus(uri: RedisURI): ReplicaRedis<K, V> = replica(uri)

    operator fun plus(host: String) = replica(host)

    override fun connectForExecute(): CompletionStage<StatefulRedisConnection<K, V>> {
        return client.connectAsync(codec, masterUri)
    }

    @Suppress("UNCHECKED_CAST")
    override fun connectForRead(): CompletionStage<StatefulRedisConnection<K, V>> {
        return MasterReplica.connectAsync(client, codec, nodes) as CompletableFuture<StatefulRedisConnection<K, V>>
    }

    override fun connectPubSub(): CompletionStage<StatefulRedisPubSubConnection<K, V>> {
        return client.connectPubSubAsync(codec, nodes.random())
    }
}