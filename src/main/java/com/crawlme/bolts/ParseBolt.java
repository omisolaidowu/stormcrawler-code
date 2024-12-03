package com.crawlme.bolts;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class ParseBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String url = tuple.getStringByField("url");
        // Simulate fetching the page content (you'll use a web crawler or HTTP client here)
        String pageContent = "<html>...</html>";  // This should be actual content from the URL
        System.out.println("Fetched content from: " + url);
        System.out.println("Page Content: " + pageContent);
        
        // Process the content, for example, extract product details (this is a simplified version)
        String productName = "Example Product";
        String productPrice = "$99.99";
        String productImage = "http://example.com/image.jpg";

        // Emitting data to the next bolt (CSV export)
        collector.emit(new Values(productName, productPrice, productImage));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("productName", "productPrice", "productImage"));
    }
}
