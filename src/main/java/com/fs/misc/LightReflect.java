package com.fs.misc;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

public class LightReflect {

    public static <T> T invoke(MethodHandle handle, Object... args) {
        try {
            return (T) handle.invokeExact(args);
        } catch (Throwable throwable) {
            throw new RuntimeException("bind failed,method:" + handle
                    + " args:" + Arrays.stream(args).map(Objects::toString).collect(Collectors.joining(";")),
                    throwable);
        }
    }

    public static MethodHandle invokable(MethodHandle handle) {
        return handle.asSpreader(Object[].class, handle.type().parameterCount())
                .asType(MethodType.methodType(
                        Object.class,
                        Object[].class
                ));
    }
}
