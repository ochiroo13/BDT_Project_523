/*
 * Copyright Rackspace Inc.
 * All Rights Reserved
 *
 *
 * The code in this project is made available as free and open source software under the terms and
 * conditions of the GNU Public License. For more information, please refer to the LICENSE text file included with this project,
 * or visit gnu.org if the license file was not included.
 * This SOFTWARE PRODUCT is provided by THE PROVIDER "as is" and "with all faults."
 * THE PROVIDER makes no representations or warranties of any kind concerning the
 * safety, suitability, lack of viruses, inaccuracies, typographical errors, or
 * other harmful components of this SOFTWARE PRODUCT. There are inherent dangers
 * in the use of any software, and you are solely responsible for determining
 * whether this SOFTWARE PRODUCT is compatible with your equipment and other
 * software installed on your equipment. You are also solely responsible for the
 * protection of your equipment and backup of your data, and THE PROVIDER will
 * not be liable for any damages you may suffer in connection with using,
 * modifying, or distributing this SOFTWARE PRODUCT.
 *
 * The code has been referred from here:https://github.com/zdata-inc/SparkSampleProject
 */

package com.rackspace.spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;

/**
 *
 * Predicts the sentiment of each tweet filtered by the keywords mentioned in
 * selection.txt. Sentiment is predicted based on comparing the words in a tweet
 * with bag of negative and positive words. The final sentiment
 * (positive/negative) is assigned based on positive and negative scores
 * assigned to it. The results are written in hdfs in twitter_data/sentiment
 * directory.
 *
 * Build instructions:
 *
 * cd TwitterSentimentSparkDemo mvn package
 *
 * This will create target folder with compiled jar:
 * TwitterSentimentAnalysis-0.0.1.jar. Change directory to target to submit the
 * job with following commands:
 *
 * spark-submit --class com.rackspace.spark.TwitterSentiment --master
 * yarn-cluster --num-executors 2 --driver-memory 1g --executor-memory 1g
 * --executor-cores 1 TwitterSentimentAnalysis-0.0.1.jar consumerKey
 * consumerSecret accessToken accessTokenSecret hdfs_output_path
 *
 * Notes: hdfs_output_path should be of this format:
 * hdfs://nodename/user/username
 *
 */

public class TwitterSentiment {

	static {
		Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
		Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(
				Level.ERROR);
	}
	private static final String TABLE_NAME = "tweet";
	private static final String CF_DETAIL = "tweet_detail";

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("Twitter Sentiment Analysis");

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
		final String hdfs_output_path = "file:///home/cloudera/spark/sentiment";

		// Prepares set of keywords by reading selection.txt file
		List<String> queryStrings = QueryWords.getWords();
		final HashSet<String> keywords = new HashSet<String>(queryStrings);

		// Creates twitter Stream
		JavaReceiverInputDStream stream = TwitterUtils.createStream(ssc);

		// Gets new Stream of RDD of the form (tweetID, tweet)
		JavaPairDStream<Long, String> tweets = stream
				.mapToPair(new TwitterFilterFunction());

		// Gets tweet that contains search terms
		JavaPairDStream<Long, String> filtered = tweets
				.filter(new Function<Tuple2<Long, String>, Boolean>() {
					private static final long serialVersionUID = 42l;

					@Override
					public Boolean call(Tuple2<Long, String> tweet) {
						if (tweet != null && !tweet._2().isEmpty()
								&& tweet._1() != null) {
							// for (String key : keywords) {
							// if (tweet._2().contains(key))
							return true;
							// }
						}
						return false;
					}
				});

		// Apply text filter to remove different languages/unwanted symbols
		// outputs the stream in the format (tweetID, tweet, filteredTweet)
		JavaDStream<Tuple3<Long, String, String>> tweetsFiltered = filtered
				.map(new TextFilter());

		// remove the stop words from each tweet and output the stream in the
		// format (tweetID, tweet, filteredTweet)
		tweetsFiltered = tweetsFiltered
				.map(new Function<Tuple3<Long, String, String>, Tuple3<Long, String, String>>() {
					@Override
					public Tuple3<Long, String, String> call(
							Tuple3<Long, String, String> tweet)
							throws Exception {
						String filterText = tweet._3();

						List<String> stopWords = StopWords.getWords();
						for (String word : stopWords) {
							filterText = filterText.replaceAll("\\b" + word
									+ "\\b", "");
						}
						return new Tuple3<Long, String, String>(tweet._1(),
								tweet._2(), filterText);
					}
				});

		// get positive score ((tweetID, tweet, filteredTweet), posScore)
		JavaPairDStream<Tuple2<Long, String>, Float> positiveTweets = tweetsFiltered
				.mapToPair(new PairFunction<Tuple3<Long, String, String>, Tuple2<Long, String>, Float>() {
					@Override
					public Tuple2<Tuple2<Long, String>, Float> call(
							Tuple3<Long, String, String> tweet)
							throws Exception {
						String text = tweet._3();
						Set<String> posWords = PositiveWords.getWords();
						String[] words = text.split(" ");
						int numWords = words.length;
						int numPosWords = 0;
						for (String word : words) {
							if (posWords.contains(word))
								numPosWords++;
						}
						return new Tuple2<Tuple2<Long, String>, Float>(
								new Tuple2<Long, String>(tweet._1(), tweet._2()),
								(float) numPosWords / numWords);
					}
				});

		// get negative score ((tweetID, tweet, filteredTweet), negScore)
		JavaPairDStream<Tuple2<Long, String>, Float> negativeTweets = tweetsFiltered
				.mapToPair(new PairFunction<Tuple3<Long, String, String>, Tuple2<Long, String>, Float>() {
					@Override
					public Tuple2<Tuple2<Long, String>, Float> call(
							Tuple3<Long, String, String> tweet)
							throws Exception {
						String text = tweet._3();
						Set<String> negWords = NegativeWords.getWords();
						String[] words = text.split(" ");
						int numWords = words.length;
						int numNegWords = 0;
						for (String word : words) {
							if (negWords.contains(word))
								numNegWords++;
						}
						return new Tuple2<Tuple2<Long, String>, Float>(
								new Tuple2<Long, String>(tweet._1(), tweet._2()),
								(float) numNegWords / numWords);
					}
				});

		// Perform join of positiveTweet and negativeTweet
		JavaPairDStream<Tuple2<Long, String>, Tuple2<Float, Float>> joined = positiveTweets
				.join(negativeTweets);

		// Transform into 4 element tuple for simplification.
		// Outputs the new stream in the format (tweetID, tweet, posScore,
		// negScore)
		JavaDStream<Tuple4<Long, String, Float, Float>> scoredTweets = joined
				.map(new Function<Tuple2<Tuple2<Long, String>, Tuple2<Float, Float>>, Tuple4<Long, String, Float, Float>>() {
					private static final long serialVersionUID = 42l;

					@Override
					public Tuple4<Long, String, Float, Float> call(
							Tuple2<Tuple2<Long, String>, Tuple2<Float, Float>> tweet) {
						return new Tuple4<Long, String, Float, Float>(tweet
								._1()._1(), tweet._1()._2(), tweet._2()._1(),
								tweet._2()._2());
					}
				});

		// Filter out neutral/unwanted results
		JavaDStream<Tuple4<Long, String, Float, Float>> filteredScoredTweets = scoredTweets
				.filter(new Function<Tuple4<Long, String, Float, Float>, Boolean>() {
					@Override
					public Boolean call(
							Tuple4<Long, String, Float, Float> scored_tweet)
							throws Exception {
						return (scored_tweet._3() > scored_tweet._4()
								|| scored_tweet._3() < scored_tweet._4() || ((scored_tweet
								._3() == scored_tweet._4()) && (scored_tweet
								._3() > 0.0 && scored_tweet._4() > 0.0)));
					}
				});

		// Identify winning sentiment and assign it to result
		// Outputs the new stream in the format (tweetID, tweet, posScore,
		// negScore, sentiment)
		JavaDStream<Tuple5<Long, String, Float, Float, String>> result = filteredScoredTweets
				.map(new ScoreTweetsFunction());

		// Write the result to HDFS
		result.foreach(new Function2<JavaRDD<Tuple5<Long, String, Float, Float, String>>, Time, Void>() {
			@Override
			public Void call(
					JavaRDD<Tuple5<Long, String, Float, Float, String>> tuple5JavaRDD,
					Time time) throws Exception {
				if (tuple5JavaRDD.count() <= 0)
					return null;
				tuple5JavaRDD.saveAsTextFile(hdfs_output_path
						+ "/twitter_data/sentiment/sentiment" + "_"
						+ time.milliseconds());
				return null;
			}
		});

		result.foreachRDD(

		new Function<JavaRDD<Tuple5<Long, String, Float, Float, String>>, Void>() {

			public Void call(
					JavaRDD<Tuple5<Long, String, Float, Float, String>> rdd)
					throws Exception {

				rdd.foreachPartition(

				new VoidFunction<Iterator<Tuple5<Long, String, Float, Float, String>>>() {

					public void call(
							Iterator<Tuple5<Long, String, Float, Float, String>> iterator)
							throws Exception {

						hBaseWriter(iterator, "tweet_detail");

					}

				}

				);

				return null;

			}

		}

		);

		result.print();
		ssc.start();
		ssc.awaitTermination();
	}

	private static void hBaseWriter(
			Iterator<Tuple5<Long, String, Float, Float, String>> iterator,
			String columnFamily) throws IOException {

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

			System.out.println(" Done!");

			table = connection.getTable(TableName.valueOf(TABLE_NAME));

			List<Get> rowList = new ArrayList<Get>();

			while (iterator.hasNext()) {

				Get get = new Get(iterator.next()._2().getBytes());

				rowList.add(get);

			}

			// Obtain data from table1.
			Result[] resultDataBuffer = table.get(rowList);

			// Configure data of table1.
			List<Put> putList = new ArrayList<Put>();

			for (int i = 0; i < resultDataBuffer.length; i++) {

				String row = new String(rowList.get(i).getRow());

				Result resultData = resultDataBuffer[i];

				if (!resultData.isEmpty()) {
					// Obtain the old value based on the cluster family and //
					// cloumn.
					String aCid = Bytes.toString(resultData.getValue(
							columnFamily.getBytes(), "cid".getBytes()));

					Put put = new Put(Bytes.toBytes(row));

					// Calculation reslut
					int resultValue = Integer.valueOf(row)
							+ Integer.valueOf(aCid);

					put.addColumn(Bytes.toBytes(columnFamily),
							Bytes.toBytes("cid"),
							Bytes.toBytes(String.valueOf(resultValue)));

					putList.add(put);
				}
			}

			if (putList.size() > 0) {
				table.put(putList);
			}

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
					// Close the Hbase connection.
					connection.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
