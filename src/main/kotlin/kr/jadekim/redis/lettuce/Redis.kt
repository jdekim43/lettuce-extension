package kr.jadekim.redis.lettuce

import io.lettuce.core.ClientOptions
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisFuture
import io.lettuce.core.RedisURI
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.resource.DefaultClientResources
import io.lettuce.core.support.AsyncConnectionPoolSupport
import io.lettuce.core.support.BoundedPoolConfig
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.asDeferred
import kotlinx.coroutines.runBlocking
import kr.jadekim.redis.lettuce.codec.KeyPrefixStringCodec
import java.io.Closeable

class Redis(
        host: String,
        port: Int = 6379,
        dbIndex: Int = 0,
        private val keyPrefix: String = "",
        poolSize: Int = BoundedPoolConfig.DEFAULT_MAX_TOTAL
) : Closeable {

    private val uri = RedisURI.Builder.redis(host, port).withDatabase(dbIndex).build()
    private val resourceConfig = DefaultClientResources.create()
    private val options = ClientOptions.builder()
            .autoReconnect(true)
            .build()

    private val client = RedisClient.create(resourceConfig, uri).apply {
        options = this@Redis.options
    }

    private val poolConfig = BoundedPoolConfig.builder()
            .minIdle(1)
            .maxIdle(poolSize)
            .maxTotal(poolSize)
            .testOnAcquire(true)
            .testOnCreate(true)
            .build()

    private val pool = AsyncConnectionPoolSupport.createBoundedObjectPool(
            { client.connectAsync(KeyPrefixStringCodec(keyPrefix), uri) },
            poolConfig
    )

    override fun close() {
        runBlocking { closeAsync() }
    }

    suspend fun closeAsync() {
        pool.closeAsync().asDeferred().await()
        client.shutdownAsync().asDeferred().await()
    }

    suspend fun pubsub(queueCapacity: Int = Channel.UNLIMITED): RedisPubSubConnection {
        return client.connectPubSubAsync(KeyPrefixStringCodec(keyPrefix), uri)
                .asDeferred()
                .await()
                .let { RedisPubSubConnection(it, queueCapacity) }
    }

    suspend fun <T> execute(statement: RedisAsyncCommands<String, String>.() -> RedisFuture<T>): T {
        return executeCommand { statement().asDeferred().await() }
    }

    suspend operator fun <T> invoke(
            statement: RedisAsyncCommands<String, String>.() -> RedisFuture<T>
    ): T = execute(statement)

    suspend fun pipe(
            statement: suspend RedisAsyncCommands<String, String>.(MutableList<RedisFuture<*>>) -> Unit
    ): List<Any> {
        val commands = mutableListOf<RedisFuture<*>>()
        val connection = pool.acquire().asDeferred().await()

        return try {
            with(connection.async()) {
                setAutoFlushCommands(false)

                statement(commands)

                flushCommands()

                setAutoFlushCommands(true)

                val deferredCommands = commands.map { it.asDeferred() }
                        .toTypedArray()

                awaitAll(*deferredCommands)
            }
        } finally {
            pool.release(connection)
        }
    }

    private suspend fun <T> executeCommand(statement: suspend RedisAsyncCommands<String, String>.() -> T): T {
        val connection = pool.acquire().asDeferred().await()

        return try {
            connection.async().statement()
        } finally {
            pool.release(connection)
        }
    }
}