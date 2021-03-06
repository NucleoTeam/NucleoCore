package com.synload.nucleo.interlink;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface InterlinkEvent {
    InterlinkEventType value();
}
