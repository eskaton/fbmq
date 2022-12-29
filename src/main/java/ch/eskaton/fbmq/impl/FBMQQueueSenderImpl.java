package ch.eskaton.fbmq.impl;

import ch.eskaton.fbmq.FBMQMessage;
import ch.eskaton.fbmq.FBMQQueue;
import ch.eskaton.fbmq.FBMQQueueSender;

public class FBMQQueueSenderImpl implements FBMQQueueSender {

    private final FBMQSessionImpl session;

    private final FBMQQueue queue;

    FBMQQueueSenderImpl(FBMQSessionImpl session, FBMQQueue queue) {
        this.session = session;
        this.queue = queue;
    }

    public void send(FBMQMessage message) throws IllegalStateException {
        session.send(queue, message);
    }

}
