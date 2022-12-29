package ch.eskaton.fbmq;

public interface FBMQSession {

    FBMQQueue createQueue(String queueName) throws FBMQException;

    FBMQMessageProducer createProducer(FBMQDestination destName);

    FBMQMessageConsumer createConsumer(FBMQDestination destName);

    FBMQMessage createByteMessage(byte[] bytes);

    void rollback() throws FBMQException, IllegalStateException;

    void commit() throws FBMQException, IllegalStateException;

    void close() throws FBMQException;

}
