package kr.jadekim.redis.lettuce.util

import com.google.gson.Gson
import kr.jadekim.redis.lettuce.Redis
import java.util.*

interface SuspendQueue<T> {

    suspend fun size(): Long

    suspend fun isEmpty(): Boolean

    suspend fun pop(): T?

    suspend fun peek(): T?

    suspend fun push(value: T)

    suspend fun push(values: List<T>)

    suspend fun clear()
}

open class RedisQueue<T>(
        private val redis: Redis<String, String>,
        val redisKey: String,
        private val typeRef: Class<T>
) : SuspendQueue<T> {

    protected val gson = Gson()

    override suspend fun size() = redis { llen(redisKey) }

    override suspend fun isEmpty() = size() == 0L

    override suspend fun pop() = deserializeSafe(redis { rpop(redisKey) })

    override suspend fun peek() = pop()

    override suspend fun push(value: T) {
        val serialized = serialize(value)

        redis { lpush(redisKey, serialized) }
    }

    override suspend fun push(values: List<T>) {
        val serialized = values.map { serialize(it) }.toTypedArray()

        redis { lpush(redisKey, *serialized) }
    }

    override suspend fun clear() {
        redis { del(redisKey) }
    }

    protected open fun serialize(data: T): String {
        return gson.toJson(data)
    }

    protected fun deserializeSafe(data: String?): T? {
        if (data.isNullOrBlank()) {
            return null
        }

        return deserialize(data)
    }

    protected open fun deserialize(data: String): T {
        return gson.fromJson(data, typeRef)
    }
}

class MockSuspendQueue<T> : PrioritySuspendQueue<T> {

    private val queue = LinkedList<T>()

    override suspend fun size(): Long = queue.size.toLong()

    override suspend fun isEmpty(): Boolean = queue.isEmpty()

    override suspend fun pop(): T? = try {
        queue.pop()
    } catch (e: NoSuchElementException) {
        null
    }

    override suspend fun peek(): T? = queue.peek()

    override suspend fun push(value: T) = queue.push(value)

    override suspend fun push(values: List<T>) {
        queue.addAll(values)
    }

    override suspend fun clear() = queue.clear()

    override suspend fun push(value: T, score: Double) = queue.push(value)

    override suspend fun push(vararg values: Pair<T, Double>) {
        queue.addAll(values.map { it.first })
    }

    override suspend fun slice(start: Long, stop: Long): List<T> = queue.subList(start.toInt(), stop.toInt())

    override suspend fun get(idx: Long): T? = queue[idx.toInt()]

    override suspend fun elements(): List<T> = queue

    override suspend fun extend(collection: Collection<Pair<T, Double>>) {
        queue.addAll(collection.map { it.first })
    }
}