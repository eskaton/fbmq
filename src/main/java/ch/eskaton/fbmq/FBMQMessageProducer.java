package ch.eskaton.fbmq;

public interface FBMQMessageProducer {

    void send(FBMQMessage message) throws FBMQException, IllegalStateException;

}
