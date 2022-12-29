package ch.eskaton.fbmq.impl;

import ch.eskaton.fbmq.FBMQConnection;
import ch.eskaton.fbmq.FBMQException;
import ch.eskaton.fbmq.FBMQMessage;
import ch.eskaton.fbmq.FBMQQueue;
import ch.eskaton.fbmq.FBMQSession;

import java.util.ArrayList;
import java.util.List;

public class FBMQInternalConnection implements FBMQConnection {

    private final List<FBMQSession> sessions = new ArrayList<FBMQSession>();

    private final FBMQBroker broker;

    public FBMQInternalConnection(String brokerName) throws FBMQException {
        this.broker = FBMQBroker.connect(brokerName, this);
    }

    public void close() throws FBMQException {
        synchronized (sessions) {
            for (var session : sessions) {
                session.close();
            }

            sessions.clear();
            broker.removeConnection(this);
        }
    }

    public FBMQSession createSession() {
        var session = new FBMQSessionImpl(this);

        synchronized (sessions) {
            sessions.add(session);
        }

        return session;
    }

    public FBMQQueue createQueue(String queueName) throws FBMQException {
        return broker.createQueue(queueName);
    }

    void send(FBMQQueue queue, FBMQTransaction transaction, FBMQMessage message) {
        broker.send(queue, transaction, message);
    }

    FBMQMessage receive(FBMQQueue queue, FBMQTransaction transaction) throws FBMQException {
        return broker.receive(queue, transaction);
    }

    FBMQTransaction createTransaction() {
        return broker.createTransaction();
    }

    void commit(FBMQTransaction transaction) throws FBMQException {
        broker.commit(transaction);
    }

    void rollback(FBMQTransaction transaction) {
        broker.rollback(transaction);
    }

}
