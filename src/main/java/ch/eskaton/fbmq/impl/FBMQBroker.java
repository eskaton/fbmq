package ch.eskaton.fbmq.impl;

import ch.eskaton.fbmq.FBMQConnection;
import ch.eskaton.fbmq.FBMQException;
import ch.eskaton.fbmq.FBMQMessage;
import ch.eskaton.fbmq.FBMQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class FBMQBroker {

    private static final Logger LOGGER = LoggerFactory.getLogger(FBMQBroker.class);

    private static final String MSG_EXTENSION = ".msg";

    static final String DEF_BROKER_NAME = "fbmq";

    private static final String DEF_DATA_DIR = "data";

    private static final String QUEUE_DIR = "queues";

    private static final String IN_FLIGHT_DIR = "inflight";

    private static final String TRX_DIR = "trx";

    private String brokerName = DEF_BROKER_NAME;

    private File dataDir = new File(DEF_DATA_DIR);

    private final File queueDir = new File(dataDir, QUEUE_DIR);

    private final File inflightDir = new File(dataDir, IN_FLIGHT_DIR);

    private final File trxDir = new File(dataDir, TRX_DIR);

    private static final Map<String, FBMQBroker> brokers = new HashMap<String, FBMQBroker>();

    private final Map<String, FBMQQueue> queues = new HashMap<String, FBMQQueue>();

    private final Set<FBMQConnection> connections = new HashSet<FBMQConnection>();

    private final Map<String, FBMQTransaction> transactions = new HashMap<String, FBMQTransaction>();

    boolean running = false;

    public void setBrokerName(String brokerName) throws FBMQException {
        if (running) {
            throw new FBMQException("Can't change the properties of a running broker");
        }

        this.brokerName = brokerName;
    }

    public void setDataDir(String dataDir) throws FBMQException {
        if (running) {
            throw new FBMQException("Can't change the properties of a running broker");
        }

        this.dataDir = new File(dataDir);
    }

    public void start() throws FBMQException {
        LOGGER.info("Starting broker " + brokerName);

        synchronized (brokers) {
            if (brokers.containsKey(brokerName)) {
                LOGGER.info("Failed to start broker {}, because it's already register", brokerName);
                throw new FBMQException("A broker with name " + brokerName + " is already running");
            }

            brokers.put(brokerName, this);
        }

        if (!dataDir.exists()
                && !dataDir.mkdir() || !queueDir.exists()
                && !queueDir.mkdir() || !inflightDir.exists()
                && !inflightDir.mkdir() || !trxDir.exists()
                && !trxDir.mkdir()) {

            synchronized (brokers) {
                brokers.remove(brokerName);
            }

            throw new FBMQException("Failed to start broker: can't create data directory");
        } else {
            LOGGER.info("Finishing open transactions...");

            int trxReplayCnt = 0;

            for (var trxFile : trxDir.listFiles()) {
                FBMQTransaction trx = null;

                try {
                    trx = FBMQTransaction.load(trxFile);

                    LOGGER.debug("Finishing transaction " + trx.getId());

                    if (trx != null) {
                        commit(trx, true);
                        trxReplayCnt++;
                    }
                } catch (IOException e) {
                    throw new FBMQException("Failed to finish pending transactions. Last transaction was " + trx, e);
                }
            }

            LOGGER.info("Finished " + trxReplayCnt + " pending transactions");
            LOGGER.debug("Deleting uncommitted messages...");

            int delMsgCount = 0;

            for (var inflightFile : inflightDir.listFiles()) {
                // delete in-flight files, which don't belong to a transaction
                LOGGER.debug("Deleting message " + inflightFile.getName());
                inflightFile.delete();
                delMsgCount++;
            }

            LOGGER.debug("Deleted " + delMsgCount + " uncommitted messages");
            LOGGER.info("Loading queues");

            for (var queue : queueDir.listFiles()) {
                if (queue.isDirectory()) {
                    var qName = queue.getName();

                    queues.put(qName, FBMQQueueImpl.queueFromDir(queue));
                }
            }
        }

        running = true;
    }

    public void stop() {
        synchronized (brokers) {
            if (brokers.containsKey(brokerName)) {
                brokers.remove(brokerName);
                queues.clear();
            }
        }
        running = false;
    }

    FBMQQueue createQueue(String qName) throws FBMQException {
        synchronized (queues) {
            if (queues.containsKey(qName)) {
                return queues.get(qName);
            }

            var qDir = new File(queueDir, qName);

            if (!qDir.mkdir()) {
                throw new FBMQException("Failed to create queue " + qName);
            }

            var queue = new FBMQQueueImpl(qDir, new LinkedList<>());

            queues.put(qName, queue);

            return queue;
        }
    }

    static FBMQBroker connect(String brokerName, FBMQConnection conn)
            throws FBMQException {
        FBMQBroker broker;

        synchronized (brokers) {
            if (brokers.containsKey(brokerName)) {
                broker = brokers.get(brokerName);
            } else {
                throw new FBMQException("Invalid broker " + brokerName);
            }
        }

        broker.addConnection(conn);

        return broker;
    }

    private void addConnection(FBMQConnection conn) {
        synchronized (connections) {
            connections.add(conn);
        }
    }

    void removeConnection(FBMQConnection conn) {
        synchronized (connections) {
            connections.remove(conn);
        }
    }

    void send(FBMQQueue queue, FBMQTransaction transaction, FBMQMessage message) {
        synchronized (queue) {
            transaction.add(new FBMQOperation(FBMQOperation.Mode.Insert, queue.getName(), message));

            try {
                ((FBMQMessageImpl) message).save(inflightDir);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public FBMQMessage receive(FBMQQueue queue, FBMQTransaction transaction) throws FBMQException {
        synchronized (queue) {
            var message = queue.receive();

            if (message != null) {
                transaction.add(new FBMQOperation(FBMQOperation.Mode.Remove, queue.getName(), message));

                var msgFileName = message.getId() + MSG_EXTENSION;

                new File(dataDir + File.separator + queueDir.getName()
                        + File.separator + queue.getName(), msgFileName)
                        .renameTo(new File(inflightDir, msgFileName));
            }

            return message;
        }
    }

    FBMQTransaction createTransaction() {
        var tx = new FBMQTransaction(UUID.randomUUID().toString());

        synchronized (transactions) {
            transactions.put(tx.getId(), tx);
        }

        return tx;
    }

    private void commit(FBMQTransaction transaction, boolean recovery) throws FBMQException {
        transaction.save(trxDir);

        for (var op : transaction.getOperations()) {
            var msgId = op.getMessage().getId();
            var msgFile = new File(inflightDir, msgId + MSG_EXTENSION);

            switch (op.getMode()) {
                case Insert:
                    var dir = new File(dataDir + File.separator
                            + queueDir.getName() + File.separator
                            + op.getQueue());

                    if (!msgFile.exists() && recovery) {
                        LOGGER.debug("Message {} isn't available. It was probably committed earlier", op.getMessage().getId());
                        continue;
                    }

                    if (!msgFile.renameTo(new File(dir, msgId + MSG_EXTENSION))) {
                        LOGGER.error("Message {} couldn't be committed to queue {}", msgId, op.getQueue());
                        continue;
                    }

                    if (!recovery) {
                        synchronized (queues) {
                            queues.get(op.getQueue()).send(op.getMessage());
                        }
                    }
                    break;
                case Remove:
                    if (!msgFile.delete()) {
                        LOGGER.debug("Received message {} couldn't be deleted", msgId);
                        continue;
                    }
                    break;
            }
        }

        transaction.delete(trxDir);
    }

    void commit(FBMQTransaction transaction) throws FBMQException {
        commit(transaction, false);
    }

    void rollback(FBMQTransaction transaction) {
        for (var op : transaction.getOperations()) {
            var msgId = op.getMessage().getId();
            var msgFile = new File(inflightDir, msgId + MSG_EXTENSION);

            switch (op.getMode()) {
                case Insert:
                    msgFile.delete();
                    break;
                case Remove:
                    var dir = new File(dataDir + File.separator
                            + queueDir.getName() + File.separator
                            + op.getQueue());
                    msgFile.renameTo(new File(dir, op.getMessage().getId() + MSG_EXTENSION));
                    break;
            }
        }
    }

}
