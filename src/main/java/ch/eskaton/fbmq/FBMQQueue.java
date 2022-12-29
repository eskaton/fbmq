package ch.eskaton.fbmq;

public interface FBMQQueue extends FBMQDestination {

	void send(FBMQMessage message) throws FBMQException;

	FBMQMessage receive() throws FBMQException;

	String getName();

}
