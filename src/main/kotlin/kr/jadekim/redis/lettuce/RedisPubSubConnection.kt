package kr.jadekim.redis.lettuce

import io.lettuce.core.pubsub.RedisPubSubListener
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.future.asDeferred
import kotlinx.coroutines.runBlocking
import kr.jadekim.logger.JLog
import java.io.Closeable

class RedisPubSubConnection internal constructor(
    private val connection: StatefulRedisPubSubConnection<String, String>,
    private val queueCapacity: Int = Channel.UNLIMITED
) : Closeable {

    private val channel = Channel<Pair<String, String>>(queueCapacity)

    init {
        val listener = object : AbstractPubSubListener() {

            override fun message(channel: String, message: String?) {
                runBlocking {
                    this@RedisPubSubConnection.channel.send(channel to (message ?: ""))
                }
            }

            override fun message(pattern: String, channel: String, message: String?) {
                runBlocking {
                    this@RedisPubSubConnection.channel.send(channel to (message ?: ""))
                }
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

    suspend fun subscribe(vararg channel: String) {
        connection.async().subscribe(*channel).asDeferred().await()
    }

    suspend fun unsubscribe(vararg channel: String) {
        connection.async().unsubscribe(*channel).asDeferred().await()
    }

    suspend fun psubscribe(vararg pattern: String) {
        connection.async().psubscribe(*pattern).asDeferred().await()
    }

    suspend fun punsubscribe(vararg pattern: String) {
        connection.async().punsubscribe(*pattern).asDeferred().await()
    }

    fun asCoroutineChannel(): ReceiveChannel<Pair<String, String>> = channel

    private abstract class AbstractPubSubListener : RedisPubSubListener<String, String> {

        private val logger = JLog.get(RedisPubSubConnection::class)

        override fun psubscribed(pattern: String, count: Long) {
            logger.info("psubscribed $pattern")
            //do nothing
        }

        override fun punsubscribed(pattern: String, count: Long) {
            logger.info("punsubscribed $pattern")
            //do nothing
        }

        override fun unsubscribed(channel: String, count: Long) {
            logger.info("unsubscribed $channel")
            //do nothing
        }

        override fun subscribed(channel: String, count: Long) {
            logger.info("subscribed $channel")
            //do nothing
        }
    }
}