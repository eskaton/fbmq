package ch.eskaton.fbmq.impl;

import ch.eskaton.fbmq.FBMQException;
import ch.eskaton.fbmq.FBMQMessage;
import ch.eskaton.fbmq.FBMQQueue;
import ch.eskaton.fbmq.FBMQQueueReceiver;

public class FBMQQueueReceiverImpl implements FBMQQueueReceiver {

    private final FBMQSessionImpl session;

    private final FBMQQueue queue;

    FBMQQueueReceiverImpl(FBMQSessionImpl session, FBMQQueue queue) {
        this.session = session;
        this.queue = queue;
    }

    public FBMQMessage receive() throws FBMQException, IllegalStateException {
        return session.receive(queue);
    }

}
