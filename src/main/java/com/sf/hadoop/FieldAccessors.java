package com.sf.hadoop;

import com.fs.misc.LightReflect;
import com.fs.misc.Promise;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class FieldAccessors {

    public interface FieldAccessor {

        Object get(Object instance);

        void set(Object Instance, Object value);
    }

    protected static ConcurrentMap<String, Promise<FieldAccessor>> ACCESSORS = new ConcurrentHashMap<>();

    public static FieldAccessor accesor(Class<?> clazz, String name) {
        String key = clazz.getName() + name;

        return ACCESSORS.compute(key, (ignore, old) -> {
            if (old != null) {
                return old;
            }

            return Promise.light(() -> {
                // get field
                Field field = clazz.getDeclaredField(name);
                field.setAccessible(true);

                // lookup
                MethodHandles.Lookup lookup = MethodHandles.lookup();
                MethodHandle getter = LightReflect.invokable(lookup.unreflectGetter(field));
                MethodHandle setter = LightReflect.invokable(lookup.unreflectSetter(field));

                return new FieldAccessor() {
                    @Override
                    public Object get(Object instance) {
                        return LightReflect.invoke(getter, instance);
                    }

                    @Override
                    public void set(Object instance, Object value) {
                        LightReflect.invoke(setter, instance, value);
                    }
                };
            });
        }).join();
    }

}
