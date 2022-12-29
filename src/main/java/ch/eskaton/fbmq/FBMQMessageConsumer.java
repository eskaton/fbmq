package ch.eskaton.fbmq;


public interface FBMQMessageConsumer {

    FBMQMessage receive() throws FBMQException, IllegalStateException;

}
