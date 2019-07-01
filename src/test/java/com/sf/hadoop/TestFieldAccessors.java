package com.sf.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;

public class TestFieldAccessors {

    public static final Log LOGGER = LogFactory.getLog(TestFieldAccessors.class);

    public static class RootClass {
        protected String field_1;
    }

    public static class ChildClass {
        protected String field_2;
    }

    @Test
    public void test() {
        RootClass root = new RootClass();
        root.field_1 = "a root class field";

        ChildClass child = new ChildClass();
        child.field_2 = "a child class field";

        Object file_2_value = FieldAccessors.accesor(ChildClass.class, "field_2").get(child);
        Assert.assertEquals(file_2_value, child.field_2);

        Object filed_1_value = FieldAccessors.accesor(RootClass.class, "field_1").get(child);
        LOGGER.info(filed_1_value);
    }
}
