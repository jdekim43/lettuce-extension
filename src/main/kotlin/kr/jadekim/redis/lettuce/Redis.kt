package kr.jadekim.redis.lettuce

import io.lettuce.core.ClientOptions
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisFuture
import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import io.lettuce.core.resource.DefaultClientResources
import io.lettuce.core.support.AsyncConnectionPoolSupport
import io.lettuce.core.support.BoundedAsyncPool
import io.lettuce.core.support.BoundedPoolConfig
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.asDeferred
import kotlinx.coroutines.runBlocking
import java.io.Closeable
import java.util.concurrent.CompletionStage

typealias StringKeyRedis<V> = Redis<String, V>

typealias StringRedis = Redis<String, String>

abstract class Redis<K, V>(
        protected val codec: RedisCodec<K, V>,
        executePoolSize: Int = BoundedPoolConfig.DEFAULT_MAX_TOTAL,
        readPoolSize: Int = BoundedPoolConfig.DEFAULT_MAX_TOTAL
) : Closeable {

    private val resourceConfig = DefaultClientResources.create()
    private val options = ClientOptions.builder()
            .autoReconnect(true)
            .build()

    private val poolConfig = BoundedPoolConfig.builder()
            .minIdle(1)
            .testOnAcquire(true)
            .testOnCreate(true)

    protected val client = RedisClient.create(resourceConfig).apply {
        options = this@Redis.options
    }

    protected val executePool = AsyncConnectionPoolSupport.createBoundedObjectPool(
            { connectForExecute() },
            poolConfig.maxIdle(executePoolSize)
                    .maxTotal(executePoolSize)
                    .build()
    )

    protected val readPool = if (readPoolSize == 0) {
        executePool
    } else {
        AsyncConnectionPoolSupport.createBoundedObjectPool(
                { connectForRead() },
                poolConfig.maxIdle(readPoolSize)
                        .maxTotal(readPoolSize)
                        .build()
        )
    }

    abstract fun connectForExecute(): CompletionStage<StatefulRedisConnection<K, V>>

    abstract fun connectForRead(): CompletionStage<StatefulRedisConnection<K, V>>

    abstract fun connectPubSub(): CompletionStage<StatefulRedisPubSubConnection<K, V>>

    suspend fun subscribe(queueCapacity: Int = Channel.UNLIMITED): RedisPubSubConnection<K, V> {
        val connection = connectPubSub().asDeferred().await()

        return RedisPubSubConnection(connection, queueCapacity)
    }

    override fun close() {
        runBlocking { closeAsync() }
    }

    suspend fun closeAsync() {
        executePool.closeAsync().asDeferred().await()
        readPool.closeAsync().asDeferred().await()
        client.shutdownAsync().asDeferred().await()
    }

    suspend fun <T> execute(statement: RedisAsyncCommands<K, V>.() -> RedisFuture<T>): T {
        return executePool { statement().asDeferred().await() }
    }

    suspend fun <T> read(statement: RedisAsyncCommands<K, V>.() -> RedisFuture<T>): T {
        return readPool { statement().asDeferred().await() }
    }

    suspend operator fun <T> invoke(
            isRead: Boolean = true,
            statement: RedisAsyncCommands<K, V>.() -> RedisFuture<T>
    ): T = execute(statement)

    suspend fun pipe(
            statement: suspend RedisAsyncCommands<K, V>.(MutableList<RedisFuture<*>>) -> Unit
    ): List<Any> = executePool {
        val commands = mutableListOf<RedisFuture<*>>()

        setAutoFlushCommands(false)

        statement(commands)

        flushCommands()

        setAutoFlushCommands(true)

        val deferredCommands = commands.map { it.asDeferred() }
                .toTypedArray()

        awaitAll(*deferredCommands)
    }

    private suspend operator fun <T> BoundedAsyncPool<StatefulRedisConnection<K, V>>.invoke(
            statement: suspend RedisAsyncCommands<K, V>.() -> T
    ): T {
        val connection = acquire().asDeferred().await()

        return try {
            connection.async().statement()
        } finally {
            release(connection)
        }
    }
}

@Suppress("FunctionName")
internal fun RedisURI(
        host: String,
        port: Int = 6379,
        dbIndex: Int = 0,
        password: String? = null
) = RedisURI.Builder.redis(host, port).withDatabase(dbIndex)
        .apply {
            if (password != null) {
                withPassword(password)
            }
        }
        .build()