package com.crawlme.bolts;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class CSVExportBolt extends BaseBasicBolt {
    private PrintWriter writer;

    @Override
    public void prepare(Map<String, Object> stormConf, TopologyContext context) {
        try {
            writer = new PrintWriter(new FileWriter("output.csv", true)); // Append mode
        } catch (IOException e) {
            throw new RuntimeException("Error initializing CSV writer", e);
        }
    }

    @SuppressWarnings("CallToPrintStackTrace")
    public void execute(Tuple input) {
        try {
            String line = String.join(",", input.getValues().toString());
            writer.println(line);
            writer.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // No output fields since this bolt writes directly to a file and doesn't emit tuples.
    }

    @Override
    public void cleanup() {
        if (writer != null) {
            writer.close();
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
