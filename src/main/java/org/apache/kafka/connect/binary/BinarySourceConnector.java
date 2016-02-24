package org.apache.kafka.connect.binary;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BinarySourceConnector extends SourceConnector {
    public static final String DIR_PATH = "tmp.path";

    private String tmp_path;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        tmp_path = props.get(DIR_PATH);
        if(tmp_path == null || tmp_path.isEmpty())
            throw new ConnectException("missing tmp.path");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return BinarySourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for(int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>();
            config.put(DIR_PATH, tmp_path);
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {

    }
}
