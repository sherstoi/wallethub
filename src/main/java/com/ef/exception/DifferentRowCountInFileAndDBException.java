package com.ef.exception;

/**
 * Created by iurii on 10/28/17.
 */
public class DifferentRowCountInFileAndDBException extends RuntimeException {
    public DifferentRowCountInFileAndDBException(String message) {
        super(message);
    }
}
