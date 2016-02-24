package org.apache.kafka.connect.binary;

import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

public class BinarySourceTask extends SourceTask {
    private final static Logger log = LoggerFactory.getLogger(BinarySourceTask.class);

    private String tmp_path;

    private TimerTask task;
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

        Timer timer = new Timer();
        timer.schedule( task , new Date(), 1000 );
        //loadOffsets("connectorname", "partitionname");
    }

    @Override
    public List<SourceRecord> poll() throws InterruptException {
        //consue here the pool
        List<SourceRecord> records = new ArrayList<>();
        return records;
    }

    @Override
    public void stop() {
    }

    private void loadOffsets(String connector, String partition) {
        offsets.putAll(context.offsetStorageReader().offset(Collections.singletonMap(connector, partition)));
    }
}
