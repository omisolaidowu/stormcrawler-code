package com.crawlme.bolts;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class CSVExportBolt extends BaseBasicBolt {

    // define the CSV file path
    private static final String CSV_FILE_PATH = "D:/ZenRows/newstormc/storm-crawler/products.csv";
    private boolean isFirstWrite = true;

    @Override
    @SuppressWarnings("CallToPrintStackTrace")
    // retrieve the scraped data information
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String productName = tuple.getStringByField("productName");
        String productPrice = tuple.getStringByField("productPrice");
        String productImage = tuple.getStringByField("productImage");

        try (PrintWriter writer = new PrintWriter(new FileWriter(CSV_FILE_PATH, !isFirstWrite))) {
            // write header only once
            if (isFirstWrite) {
                writer.println("\"Product Name\",\"Product Price\",\"Product Image\"");
                isFirstWrite = false;
            }
            writer.println(String.format("\"%s\",\"%s\",\"%s\"", productName, productPrice, productImage));
            System.out.printf("Written to CSV: %s, %s, %s%n", productName, productPrice, productImage);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
