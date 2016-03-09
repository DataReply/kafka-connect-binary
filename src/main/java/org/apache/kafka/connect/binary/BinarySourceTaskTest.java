package org.apache.kafka.connect.binary;


import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
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
 * Created by alex on 09.03.16.
 */
public class BinarySourceTaskTest {

    private BinarySourceTask task;
    private SourceTaskContext context;
    private Map<String, String> sourceProperties;

    @Before
    public void setup() {
        task = new BinarySourceTask();
        context = PowerMock.createMock(SourceTaskContext.class);
        task.initialize(context);

        sourceProperties = new HashMap<>();
        sourceProperties.put("topic", "binarytopic");
        sourceProperties.put("schema.name", "binaryschematest");
        sourceProperties.put("use.java.dirwatcher", "false");
        sourceProperties.put("filename.path", "test-source.png");
    }

    @Test
    public void test() throws Exception {
        replay();

        File file = new File("test-cmp.png");
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
        Map<String, Object> map = toJsonMap((Struct) records.get(0).value());

        byte[] bytes = (byte[]) map.get("binary");

        FileOutputStream stream = new FileOutputStream("1_" + (String) map.get("name"));
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


    private static Map<String, Object> toJsonMap(Struct struct) {
        Map<String, Object> jsonMap = new HashMap<String, Object>(0);
        List<Field> fields = struct.schema().fields();
        for (Field field : fields) {
            String fieldName = field.name();
            Schema.Type fieldType = field.schema().type();
            switch (fieldType) {
                case STRING:
                    jsonMap.put(fieldName, struct.getString(fieldName));
                    break;
                case INT32:
                    jsonMap.put(fieldName, struct.getInt32(fieldName));
                    break;
                case INT16:
                    jsonMap.put(fieldName, struct.getInt16(fieldName));
                    break;
                case INT64:
                    jsonMap.put(fieldName, struct.getInt64(fieldName));
                    break;
                case FLOAT32:
                    jsonMap.put(fieldName, struct.getFloat32(fieldName));
                    break;
                case BYTES:
                    jsonMap.put(fieldName, struct.getBytes(fieldName));
                    break;
                case STRUCT:
                    jsonMap.put(fieldName, toJsonMap(struct.getStruct(fieldName)));
                    break;
            }
        }
        return jsonMap;
    }

}