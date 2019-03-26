package com.synload.nucleo.event;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface NucleoEvent {
  String value() default "";
  String[] chains() default {};
}
