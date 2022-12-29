package ch.eskaton.fbmq.impl;

import ch.eskaton.fbmq.FBMQException;
import ch.eskaton.fbmq.FBMQMessage;
import ch.eskaton.fbmq.utils.FileChannelUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class FBMQMessageImpl implements FBMQMessage {

    private String id;

    private Map<String, String> headers = new HashMap<String, String>();

    private byte[] body;

    private boolean loadBody = false;

    private File qDir;

    FBMQMessageImpl(byte[] body) {
        this.body = body;

        id = UUID.randomUUID().toString();

        headers.put("id", id);
    }

    static FBMQMessage load(File qDir, String id) throws Exception {
        var message = new FBMQMessageImpl(id);

        message.load(qDir);

        return message;
    }

    private FBMQMessageImpl(String id) {
        this.id = id;
    }

    public byte[] getBody() throws FBMQException {
        if (loadBody) {
            var bodyFile = new File(qDir, getId() + ".msg");

            try {
                var bis = new BufferedInputStream(new FileInputStream(bodyFile));

                body = new byte[(int) bodyFile.length()];
                bis.read(body);
            } catch (Exception e) {
                throw new FBMQException(e);
            }

            loadBody = false;
        }

        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public Map<String, String> getHeaders() {
        return new HashMap<>(headers);
    }

    public String getId() {
        return id;
    }

    void load(File qDir) throws Exception {
        try (var raf = new RandomAccessFile(qDir.getAbsolutePath() + "/" + getId() + ".msg", "r")) {
            var channel = raf.getChannel();

            if (channel.size() > 0) {
                var len = FileChannelUtils.readInt(channel);

                for (int i = 0; i < len; i++) {
                    headers.put(FileChannelUtils.readPrefixedString(channel),
                            FileChannelUtils.readPrefixedString(channel));
                }
            }

            body = FileChannelUtils.readByteArray(channel);
        }

        loadBody = false;
        this.qDir = qDir;
    }

    public void save(File qDir) throws IOException {
        try (var raf = new RandomAccessFile(qDir.getAbsolutePath() + "/" + getId() + ".msg", "rw")) {
            var channel = raf.getChannel();

            FileChannelUtils.writeInt(channel, headers.size());

            for (var header : headers.entrySet()) {
                FileChannelUtils.writePrefixedString(channel, header.getKey());
                FileChannelUtils.writePrefixedString(channel, header.getValue());
            }

            FileChannelUtils.writeByteArray(channel, body);
        }
    }

    public static int byteArrayToInt(byte[] b) {
        return b[3] & 0xFF | (b[2] & 0xFF) << 8 | (b[1] & 0xFF) << 16
                | (b[0] & 0xFF) << 24;
    }

}
