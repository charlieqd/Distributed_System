package client;

public class RetryTransactionException extends Exception {
    public RetryTransactionException() {
        super();
    }

    public RetryTransactionException(Exception e) {
        super(e);
    }

    public RetryTransactionException(String message) {
        super(message);
    }
}
