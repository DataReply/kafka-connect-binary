package org.apache.kafka.connect.binary;

import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class BinarySourceTask extends SourceTask {
    private final static Logger log = LoggerFactory.getLogger(BinarySourceTask.class);

    private String tmp_path;

    private TimerTask task;
    private static Schema schema = null;
    private String schemaName;
    private String topic;
    private Map<String, Object> offsets = new HashMap<>(0);

    @Override
    public String version() {
        return new BinarySourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        tmp_path = props.get(BinarySourceConnector.DIR_PATH);
        if(tmp_path == null)
            throw new ConnectException("config tmp.path null");

        task = new DirWatcher(tmp_path, "") {
            protected void onChange(File file, String action ) {
                // here we code the action on a change
                System.out.println
                        ( "File "+ file.getName() +" action: " + action );
            }
        };

        schemaName = "filebinaryschema"; //map.get(SocketSourceConnector.SCHEMA_NAME);
        topic = "filebinary"; //map.get(SocketSourceConnector.TOPIC);

        log.trace("Creating schema");
        schema = SchemaBuilder
                .struct()
                .name(schemaName)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("binary", Schema.OPTIONAL_BYTES_SCHEMA)
                .build();
        Timer timer = new Timer();
        timer.schedule( task , new Date(), 1000 );
        //loadOffsets("connectorname", "partitionname");
    }

    @Override
    public List<SourceRecord> poll() throws InterruptException {

        List<SourceRecord> records = new ArrayList<>();
        //consume here the pool
        while (!((DirWatcher) task).getQueueFiles().isEmpty()) {
            File file = ((DirWatcher) task).getQueueFiles().poll();
            byte[] data = null;
            try {
                //transform file to byte[]
                Path path = Paths.get(file.getPath());
                data = Files.readAllBytes(path);
                log.error(String.valueOf(data.length));
            } catch (IOException e) {
                e.printStackTrace();
            }

            // creates the structured message
            Struct messageStruct = new Struct(schema);
            messageStruct.put("name", file.getName());
            messageStruct.put("binary", data);
            // creates the record
            // no need to save offsets
            SourceRecord record = new SourceRecord(Collections.singletonMap("file_binary", 0), Collections.singletonMap("0", 0), topic, messageStruct.schema(), messageStruct);
            records.add(record);
        }

        return records;
    }

    @Override
    public void stop() {
    }

    private void loadOffsets(String connector, String partition) {
        offsets.putAll(context.offsetStorageReader().offset(Collections.singletonMap(connector, partition)));
    }
}
