package ch.eskaton.fbmq.impl;

import ch.eskaton.fbmq.FBMQException;
import ch.eskaton.fbmq.FBMQMessage;

public class FBMQRecoveryMessage implements FBMQMessage {

	private final String id;

	public FBMQRecoveryMessage(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

	public byte[] getBody() throws FBMQException {
		throw new FBMQException("Illegal access to message body");
	}

}
