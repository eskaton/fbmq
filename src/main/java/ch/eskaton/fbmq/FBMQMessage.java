package ch.eskaton.fbmq;

public interface FBMQMessage {

	String getId();

	byte[] getBody() throws FBMQException;

}
