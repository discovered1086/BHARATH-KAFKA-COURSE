package com.kingshuk.messaging.kafka.kafkapoisonpillconsumer;

public class KafkaPoisonPillException extends Exception{

    public KafkaPoisonPillException(String message) {
        super(message);
    }

    public KafkaPoisonPillException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaPoisonPillException(Throwable cause) {
        super(cause);
    }
}
