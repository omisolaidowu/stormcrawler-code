package com.crawlme;

import org.apache.storm.topology.TopologyBuilder;
import org.apache.stormcrawler.ConfigurableTopology;

import com.crawlme.bolts.CSVExportBolt;
import com.crawlme.bolts.ParseBolt;
import com.crawlme.spouts.URLSpout;

public class CrawlTopology extends ConfigurableTopology {

    public static void main(String[] args) throws Exception {
        ConfigurableTopology.start(new CrawlTopology(), args);
    }

    @Override
    protected int run(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        // Spout: URLSpout
        builder.setSpout("url-spout", new URLSpout());

        // Bolt: ParseBolt
        builder.setBolt("parse-bolt", new ParseBolt())
                .shuffleGrouping("url-spout");

        // Bolt: CSVExportBolt
        builder.setBolt("csv-export", new CSVExportBolt())
                .shuffleGrouping("parse-bolt");

        // Submit topology
        return submit("crawl-topology", conf, builder);
    }
}
