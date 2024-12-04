package com.crawlme.bolts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class ParseBolt extends BaseBasicBolt {
    @Override
    @SuppressWarnings("CallToPrintStackTrace")
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        // retrieve the URL emitted by the URLSpout
        String url = tuple.getStringByField("url");
        System.out.println("Processing URL: " + url);
        
        try {
            // fetch the page content using Jsoup
            Document doc = Jsoup.connect(url).get();  // Fetches the HTML content from the URL

            System.out.println("Extracted HTML: " + doc);
            
            // select all product elements
            Elements products = doc.select(".product");
            List<Map<String, Object>> productList = new ArrayList<>();

            for (Element product : products) {
                // extract individual product details
                String productName = product.select(".product-name").text();
                String productPrice = product.select(".price").text();
                String productImage = product.select("img").attr("src");

                // add placeholders if values are missing
                if (productName.isEmpty()) productName = "Unknown Product";
                if (productPrice.isEmpty()) productPrice = "N/A";
                if (productImage.isEmpty()) productImage = "http://example.com/default.jpg";

                // create a map for the product details
                Map<String, Object> productMap = new HashMap<>();
                productMap.put("Product Name", productName);
                productMap.put("Product Price", productPrice);
                productMap.put("Product Image", productImage);

                // add the product map to the product list
                productList.add(productMap);

                // emit the product details to the next bolt
                collector.emit(new Values(productName, productPrice, productImage));
            }

            // log the complete product list for debugging
            System.out.println("Extracted Products: " + productList);

        } catch (IOException e) {
            // handle error in case the URL is not accessible
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // declare the fields that this bolt will emit
        declarer.declare(new Fields("productName", "productPrice", "productImage"));
    }
}
