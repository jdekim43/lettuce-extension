package kr.jadekim.redis.lettuce.codec

import io.lettuce.core.codec.StringCodec
import io.netty.buffer.ByteBuf
import java.nio.ByteBuffer

class KeyPrefixStringCodec(val keyPrefix: String) : StringCodec(Charsets.UTF_8) {

    override fun encodeKey(key: String?): ByteBuffer {
        return super.encodeKey(keyPrefix + key)
    }

    override fun encodeKey(key: String?, target: ByteBuf?) {
        super.encodeKey(keyPrefix + key, target)
    }
}