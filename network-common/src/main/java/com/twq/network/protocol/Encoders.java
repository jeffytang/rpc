package com.twq.network.protocol;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

/** Provides a canonical set of Encoders for simple types. */
public class Encoders {

    /** Strings are encoded with their length followed by UTF-8 bytes. */
    public static class Strings {
        public static int encodedLength(String s) {
            // 这里的 4 表示存储字符串长度的整数所占的字节数
            // 看下面的 encode 和 decode 方法
            return 4 + s.getBytes(StandardCharsets.UTF_8).length;
        }

        public static void encode(ByteBuf buf, String s) {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            buf.writeInt(bytes.length);
            buf.writeBytes(bytes);
        }

        public static String decode(ByteBuf buf) {
            int length = buf.readInt();
            byte[] bytes = new byte[length];
            buf.readBytes(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }
}
