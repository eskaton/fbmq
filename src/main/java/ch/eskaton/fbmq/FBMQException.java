package ch.eskaton.fbmq;

import java.io.IOException;

public class FBMQException extends Exception {

    public FBMQException(String message) {
        super(message);
    }

    public FBMQException(Exception e) {
        super(e);
    }

    public FBMQException(String message, IOException e) {
        super(message, e);
    }

}
