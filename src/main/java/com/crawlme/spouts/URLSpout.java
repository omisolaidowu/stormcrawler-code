package com.crawlme.spouts;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class URLSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private BufferedReader reader;

    @Override
    @SuppressWarnings("CallToPrintStackTrace")
    public void open(Map<String, Object> config, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            // Initialize the BufferedReader with the seed file path
            reader = new BufferedReader(new FileReader("resources/seed.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    @SuppressWarnings("CallToPrintStackTrace")
    public void nextTuple() {
        try {
            // Read a line (URL) from the seed file
            String url = reader.readLine();
            if (url != null) {
                // Emit the URL as a tuple
                collector.emit(new Values(url));
            } else {
                Thread.sleep(1000); // Sleep if no URLs are left
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    @SuppressWarnings("CallToPrintStackTrace")
    public void close() {
        try {
            // Close the BufferedReader
            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Declare the output field for the spout
        declarer.declare(new Fields("url"));
    }
}
