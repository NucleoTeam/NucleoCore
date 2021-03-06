package com.synload.nucleo.event;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface NucleoLink {
  String value() default "";
  String[] chains() default {};
}
