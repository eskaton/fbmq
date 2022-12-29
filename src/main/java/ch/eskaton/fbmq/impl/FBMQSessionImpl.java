package ch.eskaton.fbmq.impl;

import ch.eskaton.fbmq.FBMQDestination;
import ch.eskaton.fbmq.FBMQException;
import ch.eskaton.fbmq.FBMQMessage;
import ch.eskaton.fbmq.FBMQMessageConsumer;
import ch.eskaton.fbmq.FBMQMessageProducer;
import ch.eskaton.fbmq.FBMQQueue;
import ch.eskaton.fbmq.FBMQSession;

public class FBMQSessionImpl implements FBMQSession {

    private final FBMQInternalConnection conn;

    private FBMQTransaction transaction;

    boolean closed = false;

    public FBMQSessionImpl(FBMQInternalConnection conn) {
        this.conn = conn;
        transaction = conn.createTransaction();
    }

    public FBMQQueue createQueue(String queueName) throws FBMQException {
        return conn.createQueue(queueName);
    }

    public FBMQMessageProducer createProducer(FBMQDestination dest) {
        return new FBMQQueueSenderImpl(this, (FBMQQueue) dest);
    }

    public FBMQMessageConsumer createConsumer(FBMQDestination dest) {
        return new FBMQQueueReceiverImpl(this, (FBMQQueue) dest);
    }

    public FBMQMessage createByteMessage(byte[] bytes) {
        return new FBMQByteMessageImpl(bytes);
    }

    public void rollback() throws IllegalStateException {
        if (closed) {
            throw new IllegalStateException("Session is closed");
        }

        conn.rollback(transaction);
        transaction = conn.createTransaction();
    }

    public void commit() throws FBMQException, IllegalStateException {
        if (closed) {
            throw new IllegalStateException("Session is closed");
        }

        conn.commit(transaction);
        transaction = conn.createTransaction();
    }

    public void close() {
        closed = true;
        conn.rollback(transaction);
    }

    void send(FBMQQueue queue, FBMQMessage message) throws IllegalStateException {
        if (closed) {
            throw new IllegalStateException("Session is closed");
        }

        conn.send(queue, transaction, message);
    }

    FBMQMessage receive(FBMQQueue queue) throws FBMQException, IllegalStateException {
        if (closed) {
            throw new IllegalStateException("Session is closed");
        }

        return conn.receive(queue, transaction);
    }

}
