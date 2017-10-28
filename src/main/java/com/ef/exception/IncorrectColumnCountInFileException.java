package com.ef.exception;

/**
 * Created by iurii on 10/25/17.
 */
public class IncorrectColumnCountInFileException extends RuntimeException {
    public IncorrectColumnCountInFileException(String message) {
        super(message);
    }
}
