package org.apache.kafka.connect.binary.test;


import org.apache.kafka.connect.binary.BinarySourceTask;
import org.apache.kafka.connect.binary.utils.SchemaUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Alex Piermatteo on 09.03.16.
 */
public class BinarySourceTaskTest {

    private BinarySourceTask task;
    private SourceTaskContext context;
    private Map<String, String> sourceProperties;
    private final String TEST_PATH = "src/main/java/org/apache/kafka/connect/binary/test/";

    @Before
    public void setup() {
        task = new BinarySourceTask();
        context = PowerMock.createMock(SourceTaskContext.class);
        task.initialize(context);

        sourceProperties = new HashMap<>();
        sourceProperties.put("topic", "binarytopic");
        sourceProperties.put("schema.name", "binaryschematest");
        sourceProperties.put("use.java.dirwatcher", "false");
        sourceProperties.put("filename.path", TEST_PATH + "test-source.png");
    }

    @Test
    public void test() throws Exception {
        replay();

        File file = new File(TEST_PATH + "test-cmp.png");
        byte[] data = null;
        try {
            //transform file to byte[]
            Path path = Paths.get(file.getPath());
            data = Files.readAllBytes(path);
        } catch (IOException e) {
            e.printStackTrace();
        }

        task.start(sourceProperties);
        List<SourceRecord> records = task.poll();
        Map<String, Object> map = SchemaUtils.toJsonMap((Struct) records.get(0).value());

        byte[] bytes = (byte[]) map.get("binary");

        FileOutputStream stream = new FileOutputStream(TEST_PATH + "1_" + (String) map.get("name"));
        try {
            stream.write(bytes);
        } finally {
            stream.close();
        }

        Assert.assertEquals(data.length, bytes.length);
        task.stop();
    }

    private void replay() {
        PowerMock.replayAll();
    }

}