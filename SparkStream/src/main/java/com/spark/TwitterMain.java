package com.spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple5;

import com.hbase.ClntTweet;

public class TwitterMain {

	static {
		Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
		Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(
				Level.ERROR);
	}
	private static final String TABLE_NAME = "tweet";
	private static final String CF_DETAIL = "tweet_detail";

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Team 6 Project");

		if (args.length > 0)
			conf.setMaster(args[0]);
		else
			conf.setMaster("local[2]");

		JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(
				5000));

		System.setProperty("twitter4j.oauth.consumerKey",
				"a3IoQgVbNW188P7WbeQvndv4g");
		System.setProperty("twitter4j.oauth.consumerSecret",
				"HWfuc35yBC9Hvx6h2kO1DKhRMRNvW591Xm7Vdgl0LOgP9lPGsk");
		System.setProperty("twitter4j.oauth.accessToken",
				"72038826-cNFa8giK1WZE3SJS6Jxszf6sGYV920nrck3gX1fA3");
		System.setProperty("twitter4j.oauth.accessTokenSecret",
				"X7oiXW3WhuNCLs7mbmJhujAnnRtohT4G32lBknvm3JZk1");
		final String hdfs_output_path = "file:///home/cloudera/spark";

		// Creates twitter Stream
		JavaReceiverInputDStream stream = TwitterUtils.createStream(ssc);

		// Gets new Stream of RDD of the form (tweetId, tweetDetail)
		JavaPairDStream<Long, ClntTweet> tweets = stream.mapToPair(
				new Utility()).filter(f -> f != null);

		// Converting to Tuple5
		JavaDStream<Tuple5<Long, String, String, String, String>> result = tweets
				.map(f -> {
					return new Tuple5<Long, String, String, String, String>(
							f._1, f._2.getUsername(), f._2.getCreatedAt(), f._2
									.getTweetContent(), f._2.getHashTags());
				});

		result.foreach(f -> {
			if (f.count() <= 0) {
				return null;
			} else {
				Date d = new Date();
				f.saveAsTextFile(hdfs_output_path + "/twitter_data/tweet" + "_"
						+ d.getTime());
				return null;
			}
		});

		result.foreachRDD(r -> {
			System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~Count: " + r.count());
			saveToHbase(r);
			return null;
		});

		result.print();
		ssc.start();
		ssc.awaitTermination();
	}

	private static void saveToHbase(
			JavaRDD<Tuple5<Long, String, String, String, String>> javaR)
			throws IOException {

		Configuration conf = HBaseConfiguration.create();

		Connection connection = null;

		Table table = null;

		try {

			connection = ConnectionFactory.createConnection(conf);

			Admin admin = connection.getAdmin();
			HTableDescriptor tablee = new HTableDescriptor(
					TableName.valueOf(TABLE_NAME));
			tablee.addFamily(new HColumnDescriptor(CF_DETAIL));

			System.out.print("Creating table.... ");

			if (!admin.tableExists(tablee.getTableName())) {
				admin.createTable(tablee);
			}
			if (admin.isTableDisabled(tablee.getTableName())) {
				admin.enableTable(tablee.getTableName());
			}

			System.out.println(" Done!");

			table = connection.getTable(TableName.valueOf(TABLE_NAME));

			Tuple5<Long, String, String, String, String> item = ((Tuple5<Long, String, String, String, String>) javaR
					.first());

			List<Put> lstPut = new ArrayList<Put>();

			javaR.collect().forEach(
					action -> {

						Put put = new Put(Bytes.toBytes(action._1()));
						put.addColumn(Bytes.toBytes(CF_DETAIL),
								Bytes.toBytes("username"),
								Bytes.toBytes(action._2()));
						put.addColumn(Bytes.toBytes(CF_DETAIL),
								Bytes.toBytes("createdAt"),
								Bytes.toBytes(action._3()));
						put.addColumn(Bytes.toBytes(CF_DETAIL),
								Bytes.toBytes("tweetContent"),
								Bytes.toBytes(action._4()));
						put.addColumn(Bytes.toBytes(CF_DETAIL),
								Bytes.toBytes("hashTags"),
								Bytes.toBytes(action._5()));

						lstPut.add(put);
					});

			System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~LstPut.size: "
					+ lstPut.size());
			table.put(lstPut);

			table.close();

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			if (connection != null) {
				try {
					connection.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

}
