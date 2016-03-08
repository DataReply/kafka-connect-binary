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


/**
 * BinarySourceTask is a Task that reads changes from a directory for storage
 * new binary detected files in Kafka.
 *
 * @author Alex Piermatteo
 */
public class BinarySourceTask extends SourceTask {
    private final static Logger log = LoggerFactory.getLogger(BinarySourceTask.class);

    private String tmp_path;

    private TimerTask task;
    private static Schema schema = null;
    private String schemaName;
    private String topic;
    private String check_dir_ms;
    private String filename_path;
    private String use_dirwatcher;


    @Override
    public String version() {
        return new BinarySourceConnector().version();
    }

    /**
     * Start the Task. Handles configuration parsing and one-time setup of the Task.
     *
     * @param props initial configuration
     */
    @Override
    public void start(Map<String, String> props) {
        use_dirwatcher = props.get(BinarySourceConnector.USE_DIRWATCHER);
        if(use_dirwatcher == null)
            throw new ConnectException("config use.java.dirwatcher null");
        schemaName = props.get(BinarySourceConnector.SCHEMA_NAME);
        if(schemaName == null)
            throw new ConnectException("config schema.name null");
        topic = props.get(BinarySourceConnector.TOPIC);
        if(topic == null)
            throw new ConnectException("config topic null");

        if (use_dirwatcher == "true") {
            tmp_path = props.get(BinarySourceConnector.DIR_PATH);
            if(tmp_path == null)
                throw new ConnectException("config tmp.path null");
            check_dir_ms = props.get(BinarySourceConnector.CHCK_DIR_MS);

            task = new DirWatcher(tmp_path, "") {
                protected void onChange(File file, String action ) {
                    // here we code the action on a change
                    System.out.println
                            ( "File "+ file.getName() +" action: " + action );
                }
            };
            Timer timer = new Timer();
            timer.schedule( task , new Date(), Long.parseLong(check_dir_ms));
        }
        else if (use_dirwatcher == "false") {
            filename_path = props.get(BinarySourceConnector.FILE_PATH);
            if(filename_path == null || filename_path.isEmpty())
                throw new ConnectException("missing filename.path");
        }

        log.trace("Creating schema");
        schema = SchemaBuilder
                .struct()
                .name(schemaName)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("binary", Schema.OPTIONAL_BYTES_SCHEMA)
                .build();
    }


    /**
     * Poll this BinarySourceTask for new records.
     *
     * @return a list of source records
     * @throws InterruptedException
     */
    @Override
    public List<SourceRecord> poll() throws InterruptException {

        List<SourceRecord> records = new ArrayList<>();

        if (use_dirwatcher == "true") {
            //consume here the pool
            while (!((DirWatcher) task).getQueueFiles().isEmpty()) {
                File file = ((DirWatcher) task).getQueueFiles().poll();
                // creates the record
                // no need to save offsets
                SourceRecord record = create_binary_record(file);
                records.add(record);
            }
        }
        else if (use_dirwatcher == "false") {
            File file = new File(filename_path);
            // creates the record
            // no need to save offsets
            SourceRecord record = create_binary_record(file);
            records.add(record);
            this.stop();
        }

        return records;
    }


    /**
     * Create a new SourceRecord from a File
     *
     * @return a source records
     */
    private SourceRecord create_binary_record(File file) {

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
        return new SourceRecord(Collections.singletonMap("file_binary", 0), Collections.singletonMap("0", 0), topic, messageStruct.schema(), messageStruct);
    }


    /**
     * Signal this SourceTask to stop.
     */
    @Override
    public void stop() {
        if (use_dirwatcher == "true")
            task.cancel();
    }

}
