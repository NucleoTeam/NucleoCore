package com.synload.nucleo.hub;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface HubEvent {
    HubEventType value();
}
