package ch.eskaton.fbmq.impl;

import ch.eskaton.fbmq.FBMQException;
import ch.eskaton.fbmq.utils.FileChannelUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.List;

public class FBMQTransaction {

    private final String trxId;

    private List<FBMQOperation> operations = new LinkedList<FBMQOperation>();

    public FBMQTransaction(String txId) {
        this.trxId = txId;
    }

    public String getId() {
        return trxId;
    }

    public void add(FBMQOperation op) {
        operations.add(op);
    }

    public List<FBMQOperation> getOperations() {
        return operations;
    }

    public void save(File trxDir) throws FBMQException {
        try (var raf = new RandomAccessFile(trxDir.getAbsolutePath() + "/" + getId() + ".trx", "rw")) {
            var channel = raf.getChannel();

            FileChannelUtils.writePrefixedString(channel, trxId);
            FileChannelUtils.writeInt(channel, operations.size());

            for (var operation : operations) {
                FileChannelUtils.writeByte(channel, (byte) (operation.getMode().ordinal() & 0xFF));
                FileChannelUtils.writePrefixedString(channel, operation.getMessage().getId());
                FileChannelUtils.writePrefixedString(channel, operation.getQueue());
            }
        } catch (Exception e) {
            throw new FBMQException(e);
        }
    }

    public void delete(File trxDir) {
        new File(trxDir, getId() + ".trx").delete();
    }

    public static FBMQTransaction load(File trxFile) throws IOException {
        try (var raf = new RandomAccessFile(trxFile, "r")) {
            var channel = raf.getChannel();

            if (channel.size() > 0) {
                var trxId = FileChannelUtils.readPrefixedString(channel);
                int opCount = FileChannelUtils.readInt(channel);
                var operations = new LinkedList<FBMQOperation>();

                for (int i = 0; i < opCount; i++) {
                    var mode = FBMQOperation.Mode.values()[(int) FileChannelUtils.readByte(channel)];
                    var msgId = FileChannelUtils.readPrefixedString(channel);
                    var queue = FileChannelUtils.readPrefixedString(channel);

                    operations.add(new FBMQOperation(mode, queue, new FBMQRecoveryMessage(msgId)));
                }

                var trx = new FBMQTransaction(trxId);

                trx.operations = operations;

                return trx;
            } else {
                trxFile.delete();
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[trxId=" + trxId + ",operations="
                + operations + "]";
    }

}
