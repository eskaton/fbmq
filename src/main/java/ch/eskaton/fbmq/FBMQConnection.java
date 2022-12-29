package ch.eskaton.fbmq;


public interface FBMQConnection {

	void close() throws FBMQException;

	FBMQSession createSession() throws FBMQException;

}
