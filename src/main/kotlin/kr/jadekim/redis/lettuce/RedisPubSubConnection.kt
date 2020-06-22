package kr.jadekim.redis.lettuce

import io.lettuce.core.pubsub.RedisPubSubListener
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.future.asDeferred
import kotlinx.coroutines.runBlocking
import kr.jadekim.logger.JLog
import java.io.Closeable

class RedisPubSubConnection<K, V> internal constructor(
        private val connection: StatefulRedisPubSubConnection<K, V>,
        queueCapacity: Int = Channel.UNLIMITED
) : Closeable {

    private val channel = Channel<Pair<K, V>>(queueCapacity)

    init {
        val listener = object : AbstractPubSubListener<K, V>() {

            override fun message(channel: K, message: V?) {
                if (message == null) {
                    return
                }

                runBlocking {
                    this@RedisPubSubConnection.channel.send(channel to message)
                }
            }

            override fun message(pattern: K, channel: K, message: V?) {
                message(channel, message)
            }
        }

        connection.addListener(listener)
    }

    override fun close() {
        runBlocking { closeAsync() }
    }

    suspend fun closeAsync() {
        connection.closeAsync().asDeferred().await()
        channel.close()
    }

    suspend fun subscribe(vararg channel: K) {
        connection.async().subscribe(*channel).asDeferred().await()
    }

    suspend fun unsubscribe(vararg channel: K) {
        connection.async().unsubscribe(*channel).asDeferred().await()
    }

    suspend fun psubscribe(vararg pattern: K) {
        connection.async().psubscribe(*pattern).asDeferred().await()
    }

    suspend fun punsubscribe(vararg pattern: K) {
        connection.async().punsubscribe(*pattern).asDeferred().await()
    }

    fun asCoroutineChannel(): ReceiveChannel<Pair<K, V>> = channel

    private abstract class AbstractPubSubListener<K, V> : RedisPubSubListener<K, V> {

        private val logger = JLog.get(RedisPubSubConnection::class)

        override fun psubscribed(pattern: K, count: Long) {
            logger.info("psubscribed $pattern")
            //do nothing
        }

        override fun punsubscribed(pattern: K, count: Long) {
            logger.info("punsubscribed $pattern")
            //do nothing
        }

        override fun unsubscribed(channel: K, count: Long) {
            logger.info("unsubscribed $channel")
            //do nothing
        }

        override fun subscribed(channel: K, count: Long) {
            logger.info("subscribed $channel")
            //do nothing
        }
    }
}