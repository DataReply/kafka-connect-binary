package org.apache.kafka.connect.binary;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.binary.utils.StringUtils;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * BinarySourceConnector implements the connector interface
 * to write on Kafka binary files
 *
 * @author Alex Piermatteo
 */
public class BinarySourceConnector extends SourceConnector {
    public static final String USE_DIRWATCHER = "use.java.dirwatcher";

    public static final String DIR_PATH = "tmp.path";
    public static final String CHCK_DIR_MS = "check.dir.ms";

    public static final String SCHEMA_NAME = "schema.name";
    public static final String TOPIC = "topic";

    public static final String FILE_PATHS = "filename.paths";

    private String tmp_path;
    private String check_dir_ms;
    private String schema_name;
    private String topic;
    private String use_dirwatcher;
    private String filename_paths;

    private TimerTask task;
    private static ConcurrentLinkedQueue<File> queue_files;

    /**
     * Get the version of this connector.
     *
     * @return the version, formatted as a String
     */
    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }


    /**
     * Start this Connector. This method will only be called on a clean Connector, i.e. it has
     * either just been instantiated and initialized or {@link #stop()} has been invoked.
     *
     * @param props configuration settings
     */
    @Override
    public void start(Map<String, String> props) {
        use_dirwatcher = props.get(USE_DIRWATCHER);
        if(use_dirwatcher == null || use_dirwatcher.isEmpty())
            throw new ConnectException("missing use.java.dirwatcher");
        schema_name = props.get(SCHEMA_NAME);
        if(schema_name == null || schema_name.isEmpty())
            throw new ConnectException("missing schema.name");
        topic = props.get(TOPIC);
        if(topic == null || topic.isEmpty())
            throw new ConnectException("missing topic");

        if (use_dirwatcher == "true") {
            tmp_path = props.get(DIR_PATH);
            if(tmp_path == null || tmp_path.isEmpty())
                throw new ConnectException("missing tmp.path");
            check_dir_ms = props.get(CHCK_DIR_MS);
            if(check_dir_ms == null || check_dir_ms.isEmpty())
                check_dir_ms = "1000";
            filename_paths = props.get(FILE_PATHS);
            if(filename_paths == null || filename_paths.isEmpty())
                filename_paths = "";

            queue_files = new ConcurrentLinkedQueue<File>();
            task = new DirWatcher(tmp_path, "") {
                protected void onChange(File file, String action) {
                    // here we code the action on a change
                    System.out.println( "File "+ file.getName() +" action: " + action );

                    if(action == "add")
                        queue_files.add(file);
                }
            };
            Timer timer = new Timer();
            timer.schedule(task , new Date(), Long.parseLong(check_dir_ms));
        }
        else if (use_dirwatcher == "false") {
            tmp_path = props.get(DIR_PATH);
            if(tmp_path == null || tmp_path.isEmpty())
                tmp_path = "";
            check_dir_ms = props.get(CHCK_DIR_MS);
            if(check_dir_ms == null || check_dir_ms.isEmpty())
                check_dir_ms = "";
            filename_paths = props.get(FILE_PATHS);
            if(filename_paths == null || filename_paths.isEmpty())
                throw new ConnectException("missing filename.paths");
        }

    }


    /**
     * Returns the Task implementation for this Connector.
     *
     * @return tha Task implementation Class
     */
    @Override
    public Class<? extends Task> taskClass() {
        return BinarySourceTask.class;
    }


    /**
     * Returns a set of configurations for the Task based on the current configuration.
     * It always creates a single set of configurations.
     *
     * @param maxTasks maximum number of configurations to generate
     * @return configurations for the Task
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        List<String> files = Arrays.asList(filename_paths.split(","));
        int numGroups = Math.min(files.size(), maxTasks);
        List<List<String>> filesGrouped = ConnectorUtils.groupPartitions(files, numGroups);

        for(int i = 0; i < numGroups; i++) {
            Map<String, String> config = new HashMap<>();
            config.put(USE_DIRWATCHER, use_dirwatcher);
            config.put(DIR_PATH, tmp_path);
            config.put(CHCK_DIR_MS, check_dir_ms);
            config.put(FILE_PATHS, StringUtils.join(filesGrouped.get(i), ","));
            config.put(SCHEMA_NAME, schema_name);
            config.put(TOPIC, topic);
            configs.add(config);
        }
        return configs;
    }


    /**
     * Expose the files queue
     */
    public static ConcurrentLinkedQueue<File> getQueueFiles() {
        return queue_files;
    }


    /**
     * Stop this connector.
     */
    @Override
    public void stop() {

    }

}
