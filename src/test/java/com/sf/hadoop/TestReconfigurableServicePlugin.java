package com.sf.hadoop;

import com.google.gson.GsonBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestReconfigurableServicePlugin {

    public static final Log LOGGER = LogFactory.getLog(TestReconfigurableServicePlugin.class);

    @Test
    public void testListJson() {
        List<List<String>> table = new LinkedList<>();
        IntStream.range(0, 10)
                .forEach((row) -> {
                    table.add(
                            IntStream.range(0, 20)
                                    .mapToObj((column) -> "cell-" + row + "-" + column)
                                    .collect(Collectors.toList())
                    );
                });

        LOGGER.info(new GsonBuilder().setPrettyPrinting()
                .create().toJson(table));

        LOGGER.info(ReconfigurableServicePlugin.loadTemplate());
    }
}
