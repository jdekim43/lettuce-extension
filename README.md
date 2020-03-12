# lettuce-extension
* StringCodec lettuce wrapper
* KeyPrefixStringCodec
* Redis Queue
* Redis Priority Queue

## Install
### Gradle Project
1. Add dependency
    ```
    build.gradle.kts
   
    implementation("kr.jadekim:lettuce-extension:0.0.2")
    ```

## How to use
### StringCodec lettuce wrapper
```
val redis = Redis(
    host = "...",
    port = 6379,
    dbIndex = 0,
    keyPrefix = "PREFIX:",
    poolSize = 8 //pipe pool size
)

redis.set("KEY1", "VALUE") // key = PREFIX:KEY1
```
### RedisQueue
```
data class QueueItem(
    ...
)

val queue = RedisQueue(redis, "QUEUE", QueueItem::class.java)

//Operations
queue.size()
queue.isEmpty()
queue.pop()
queue.peek()
queue.push(QueueItem(...))
queue.push(listOf(QueueItem(...)))
queue.clear
```
### PriorityRedisQueue
```
data class QueueItem(
    ...
)

val queue = RedisQueue(redis, "QUEUE", QueueItem::class.java)

val priority = 100.0

//Operations
queue.size()
queue.isEmpty()
queue.pop()
queue.peek()
queue.push(QueueItem(...), priority)
queue.push(QueueItem(...) to priority, QueueItem(...) to priority, ...)
queue.slice(start, stop)
queue.get(index)
queue.elements()
queue.extend(listOf(QueueItem(...) to priority))
queue.clear()
```