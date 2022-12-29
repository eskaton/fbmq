package ch.eskaton.fbmq.impl;

import ch.eskaton.fbmq.FBMQConnection;
import ch.eskaton.fbmq.FBMQException;

public class FBMQConnectionFactory {

    public FBMQConnection createConnection(String url) throws FBMQException {
        if (url.startsWith("vm://")) {
            var brokerName = url.substring(5);

            return new FBMQInternalConnection(brokerName.length() > 0 ? brokerName : FBMQBroker.DEF_BROKER_NAME);
        }

        throw new FBMQException("Invalid connection URL provided");
    }

}
