package ch.eskaton.fbmq.impl;

import java.io.File;
import java.util.LinkedList;

import ch.eskaton.fbmq.FBMQException;
import ch.eskaton.fbmq.FBMQMessage;
import ch.eskaton.fbmq.FBMQQueue;

public class FBMQQueueImpl implements FBMQQueue {

	private final LinkedList<FBMQMessage> messages;

	private final File qDir;

	public FBMQQueueImpl(File qDir, LinkedList<FBMQMessage> messages) {
		this.qDir = qDir;
		this.messages = messages;
	}

	public static FBMQQueue queueFromDir(File qDir) throws FBMQException {
		var messages = new LinkedList<FBMQMessage>();

		for (var msgFile : qDir.listFiles()) {
			if (msgFile.getName().endsWith(".msg")) {
				try {
					messages.add(FBMQMessageImpl.load(qDir, msgFile.getName()
							.substring(0, msgFile.getName().lastIndexOf("."))));
				} catch (Exception e) {
					throw new FBMQException(e);
				}
			}
		}

		return new FBMQQueueImpl(qDir, messages);
	}

	public void send(FBMQMessage message) throws FBMQException {
		messages.addFirst(message);
	}

	public FBMQMessage receive() throws FBMQException {
		if (messages.isEmpty()) {
			return null;
		}

		return messages.removeLast();
	}

	public String getName() {
		return qDir.getName();
	}

}
