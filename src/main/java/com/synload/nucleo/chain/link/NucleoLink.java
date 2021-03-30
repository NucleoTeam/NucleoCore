package com.synload.nucleo.chain.link;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface NucleoLink {
  String value() default "";
  String[] chains() default {};
  boolean linkOnly() default false;
  boolean always() default false;
  boolean binder() default false;
}
