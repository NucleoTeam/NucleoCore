package com.synload.nucleo.chain.link;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Repeatable(NucleoRequirement.NucleoRequirements.class)
public @interface NucleoRequirement {
    String value() default "";
    String[] chains() default {};
    boolean linkOnly() default false;
    boolean acceptPreviousLinks() default true;
    boolean immediateFollows() default false;
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @interface NucleoRequirements{
        NucleoRequirement[] value();
    }
}
