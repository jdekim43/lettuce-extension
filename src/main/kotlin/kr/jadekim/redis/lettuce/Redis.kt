package kr.jadekim.redis.lettuce

import io.lettuce.core.*
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.codec.StringCodec
import io.lettuce.core.resource.DefaultClientResources
import io.lettuce.core.support.AsyncConnectionPoolSupport
import io.lettuce.core.support.BoundedPoolConfig
import io.netty.buffer.ByteBuf
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.asDeferred
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kr.jadekim.redis.lettuce.codec.KeyPrefixStringCodec
import java.io.Closeable
import java.nio.ByteBuffer
import java.time.Duration
import kotlin.coroutines.coroutineContext

class Redis(
    private val host: String,
    private val port: Int = 6379,
    private val dbIndex: Int = 0,
    private val keyPrefix: String = "",
    private val poolSize: Int = 8
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
        .minIdle(0)
        .maxIdle(poolSize)
        .maxTotal(poolSize)
        .testOnAcquire(true)
        .testOnCreate(true)
        .build()

    private val pool = AsyncConnectionPoolSupport.createBoundedObjectPool(
        { client.connectAsync(KeyPrefixStringCodec(keyPrefix), uri) },
        poolConfig
    )

    private val mainConnection by lazy {
        client.connect(KeyPrefixStringCodec(keyPrefix), uri)
    }

    override fun close() {
        runBlocking { closeAsync() }
    }

    suspend fun closeAsync() {
        pool.closeAsync().asDeferred().await()
        client.shutdownAsync().asDeferred().await()
    }

    suspend fun ping() = execute { ping() }

    suspend fun set(key: String, value: String, expire: Duration? = null) {
        pipe {
            it.add(set(key, value))

            if (expire != null) {
                it.add(expire(key, expire.seconds))
            }
        }
    }

    suspend fun get(key: String): String? {
        return read { get(key) }
    }

    suspend fun set(vararg pairs: Pair<String, String>, expire: Duration? = null) {
        if (pairs.isEmpty()) {
            return
        }

        val data = pairs.toList()

        pipe {
            it.add(mset(data.toMap()))

            if (expire != null) {
                it.addAll(data.map { each -> expire(each.first, expire.seconds) })
            }
        }
    }

    suspend fun get(vararg keys: String): Map<String, String?> {
        if (keys.isEmpty()) {
            return emptyMap()
        }

        return readCommand {
            mget(*keys).asDeferred().await()
                .filter { it.hasValue() }
                .map { Pair(it.key, it.value) }
                .toMap()
        }
    }

    suspend fun getAndSet(key: String, value: String): String? {
        return execute { getset(key, value) }
    }

    suspend fun delete(vararg key: String) {
        pipe {
            it.addAll(key.map { each -> del(each) })
        }
    }

    suspend fun exists(key: String): Boolean {
        return read { exists(key) } > 0
    }

    suspend fun expire(key: String, expire: Duration) {
        execute { expire(key, expire.seconds) }
    }

    suspend fun lpush(key: String, vararg value: String) {
        execute { lpush(key, *value) }
    }

    suspend fun lrange(key: String, start: Long, end: Long): List<String> {
        return read { lrange(key, start, end) }
    }

    suspend fun lpop(key: String): String? {
        return execute { lpop(key) }
    }

    suspend fun brpop(key: String, timeout: Long = 0): String? {
        return execute { brpop(timeout, key) }?.value
    }

    suspend fun rpop(key: String): String? {
        return execute { rpop(key) }
    }

    suspend fun lindex(key: String): String? {
        return read { lindex(key, -1) }?.let { it }
    }

    suspend fun llen(key: String): Long {
        return read { llen(key) }
    }

    suspend fun zcard(key: String): Long {
        return read { zcard(key) }
    }

    suspend fun zrange(key: String, start: Long, stop: Long): List<String> {
        return read { zrange(key, start, stop) }
    }

    suspend fun zadd(key: String, score: Double, value: String) {
        execute { zadd(key, score, value) }
    }

    suspend fun zadd(key: String, vararg scoreAndValue: ScoredValue<String>) {
        execute { zadd(key, *scoreAndValue) }
    }

    suspend fun pubsub(queueCapacity: Int = Channel.UNLIMITED): RedisPubSubConnection {
        return client.connectPubSubAsync(KeyPrefixStringCodec(keyPrefix), uri)
            .asDeferred()
            .await()
            .let { RedisPubSubConnection(it, queueCapacity) }
    }

    suspend fun publish(channel: String, message: String) {
        execute { publish(channel, message) }
    }

    suspend fun <T> execute(statement: RedisAsyncCommands<String, String>.() -> RedisFuture<T>): T {
        return executeCommand { statement().asDeferred().await() }
    }

    suspend fun <T> read(statement: RedisAsyncCommands<String, String>.() -> RedisFuture<T>): T {
        return readCommand { statement().asDeferred().await() }
    }

    suspend fun pipe(statement: suspend RedisAsyncCommands<String, String>.(MutableList<RedisFuture<*>>) -> Unit): List<Any> {
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
        return withContext(coroutineContext + Dispatchers.IO) {
            mainConnection.async().statement()
        }
    }

    private suspend fun <T> readCommand(statement: suspend RedisAsyncCommands<String, String>.() -> T): T {
        return executeCommand(statement)
    }
}