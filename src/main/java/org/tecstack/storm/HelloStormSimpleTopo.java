package org.tecstack.storm;

import java.util.Map;
import java.util.Random;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class HelloStormSimpleTopo {

	public static class HelloStormBolt extends BaseBasicBolt {

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			String word = (String) input.getValue(0);
			String out = "I'm " + word + "!";
			System.out.println("out=" + out);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer outFD) {
			// TODO Auto-generated method stub

		}

	}

	public static class HelloStormSpout extends BaseRichSpout {

		private SpoutOutputCollector collector;
		private static String[] words = { "happy", "excited", "angry" };

		@Override
		public void nextTuple() {
			String word = words[new Random().nextInt(words.length)];
			collector.emit(new Values(word));
		}

		@Override
		public void open(Map conf, TopologyContext tc,
				SpoutOutputCollector soutCollector) {
			this.collector = soutCollector;
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer outFD) {
			outFD.declare(new Fields("randomstring"));
		}

	}

	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new HelloStormSpout());
		builder.setBolt("bolt", new HelloStormBolt()).shuffleGrouping("spout");
		Config conf = new Config();
		conf.setDebug(false);
		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} else {

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("firstTopo", conf, builder.createTopology());
			Utils.sleep(10000);
			cluster.killTopology("firstTopo");
			cluster.shutdown();
		}

	}
}
