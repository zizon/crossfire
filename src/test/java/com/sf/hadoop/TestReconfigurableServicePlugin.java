package com.sf.hadoop;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.GsonBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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

        LOGGER.info(new GsonBuilder()
                .setPrettyPrinting()
                .create()
                .toJson(ImmutableMap.builder()
                        .put(
                                "datanodes",
                                ImmutableList.builder().add(
                                        ImmutableMap.builder()
                                                .put("location", "default")
                                                .put("address", "localhost")
                                                .build()
                                ).build()
                        ).build()
                ));

        //LOGGER.info(ReconfigurableServicePlugin.loadTemplate());
    }
}
