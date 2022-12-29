package ch.eskaton.fbmq.impl;

import ch.eskaton.fbmq.FBMQMessage;

public class FBMQOperation {

	enum Mode {
		Insert, Remove
	};

	private Mode mode;

	private String queue;

	private FBMQMessage message;

	public FBMQOperation(Mode mode, String queue, FBMQMessage message) {
		this.mode = mode;
		this.queue = queue;
		this.message = message;
	}

	public Mode getMode() {
		return mode;
	}

	public String getQueue() {
		return queue;
	}

	public FBMQMessage getMessage() {
		return message;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + "[mode=" + mode.toString()
				+ ",queue=" + queue + ",msgId=" + message.getId() + "]";
	}

}
