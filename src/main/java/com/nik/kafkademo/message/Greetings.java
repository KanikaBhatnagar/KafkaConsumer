package com.nik.kafkademo.message;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Greetings {

    String messageId;
    String messageTo;
    String messageFrom;
    String message;
}
