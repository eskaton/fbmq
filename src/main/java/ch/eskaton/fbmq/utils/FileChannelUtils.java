package ch.eskaton.fbmq.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileChannelUtils {

    public static void writePrefixedString(FileChannel channel, String str) throws IOException {
        var len = ByteBuffer.allocate(4);

        len.putInt(str.length());
        len.flip();

        channel.write(len);
        channel.write(ByteBuffer.wrap(str.getBytes()));
    }

    public static String readPrefixedString(FileChannel channel) throws IOException {
        var len = ByteBuffer.allocate(4);

        channel.read(len);
        len.flip();

        var strLen = len.asIntBuffer().get();

        if (strLen > 0) {
            var buf = ByteBuffer.allocate(strLen);

            channel.read(buf);

            return new String(buf.array());
        }

        return null;
    }

    public static void writeByteArray(FileChannel channel, byte[] bytes) throws IOException {
        FileChannelUtils.writeInt(channel, bytes.length);

        channel.write(ByteBuffer.wrap(bytes));
    }

    public static byte[] readByteArray(FileChannel channel) throws IOException {
        int len = FileChannelUtils.readInt(channel);
        var buf = ByteBuffer.allocate(len);

        channel.read(buf);

        var byteBuf = new byte[len];

        buf.flip();
        buf.get(byteBuf);

        return byteBuf;
    }

    public static void writeByte(FileChannel channel, byte b) throws IOException {
        var buf = ByteBuffer.allocate(1);

        buf.put(b);
        buf.flip();

        channel.write(buf);
    }

    public static byte readByte(FileChannel channel) throws IOException {
        var buf = ByteBuffer.allocate(1);

        channel.read(buf);

        buf.flip();

        return buf.get();
    }

    public static void writeInt(FileChannel channel, int i) throws IOException {
        var buf = ByteBuffer.allocate(4);

        buf.putInt(i);
        buf.flip();

        channel.write(buf);
    }

    public static int readInt(FileChannel channel) throws IOException {
        var buf = ByteBuffer.allocate(4);

        channel.read(buf);
        buf.flip();

        return buf.asIntBuffer().get();
    }

}
