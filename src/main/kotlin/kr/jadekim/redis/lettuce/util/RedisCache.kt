package kr.jadekim.redis.lettuce.util

import com.google.gson.Gson
import kr.jadekim.gson.fromJson
import kr.jadekim.redis.lettuce.Redis
import java.time.Duration

class RedisCache(
        val redis: Redis<String, String>
) {

    var gson = Gson()

    suspend inline operator fun <reified T> invoke(
            keys: List<String>,
            expire: Duration = Duration.ofSeconds(10),
            bulk: () -> List<T> = { keys.map(each) },
            noinline each: (String) -> T
    ): List<T> {
        val hit = redis.read { mget(*keys.toTypedArray()) }
        if (hit != null && hit.size == keys.size && hit.all { it.hasValue() }) {
            return hit.map { gson.fromJson(it.value) }
        }

        if (hit == null || hit.all { !it.hasValue() }) {
            return bulk().also {
                val values = it.map { value -> gson.toJson(value) }

                redis.pipe {
                    mset(keys.zip(values).toMap())

                    for (key in keys) {
                        expire(key, expire.seconds)
                    }
                }
            }
        }

        val miss = mutableMapOf<String, T>()
        val values = keys.map { key ->
            hit.firstOrNull { it.key == key }?.value
                    ?.let { gson.fromJson(it) }
                    ?: { each(key).also { value -> miss[key] = value } }()
        }

        redis.pipe {
            mset(miss.mapValues { gson.toJson(it) })

            for (key in miss.keys) {
                expire(key, expire.seconds)
            }
        }

        return values
    }

    suspend inline operator fun <reified T> invoke(
            key: String,
            expire: Duration = Duration.ofSeconds(10),
            block: () -> T
    ): T {
        val hit = redis.read { get(key) }

        if (hit != null) {
            return gson.fromJson(hit)
        }

        return block().also { result ->
            redis.pipe {
                set(key, gson.toJson(result))
                expire(key, expire.seconds)
            }
        }
    }
}