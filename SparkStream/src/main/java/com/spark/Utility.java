package com.spark;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import twitter4j.HashtagEntity;
import twitter4j.Status;

import com.hbase.ClntTweet;

public class Utility implements PairFunction<Status, String, ClntTweet> {
	private static final long serialVersionUID = 42l;

	private static final DateFormat DATEFORMAT = new SimpleDateFormat(
			"yyyy-MM-dd hh:mm:ss");

	@Override
	public Tuple2<String, ClntTweet> call(Status status) {

		try {
			if (status != null && status.getText() != null) {
				String id = status.getId() + "";
				String strDate = DATEFORMAT.format(status.getCreatedAt());
				String username = status.getUser().getScreenName();

				String text = status.getText();

				text = text.replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase()
						.replaceAll("(\\r\\n)", " ")
						.replaceAll("(\\r|\\n)", " ");

				StringBuilder hashTags = new StringBuilder();

				for (HashtagEntity hash : status.getHashtagEntities()) {
					hashTags.append(hash.getText()).append(",");
				}
				if (hashTags.toString().equals("")) {
					return null;
				}

				ClntTweet tw = new ClntTweet();
				tw.setId(id);
				tw.setUsername(username);
				tw.setCreatedAt(strDate);
				tw.setTweetContent(text);
				tw.setHashTags(hashTags.substring(0, hashTags.length() - 1));

				return new Tuple2<String, ClntTweet>(id, tw);
			}
			return null;
		} catch (Exception ex) {
			Logger LOG = Logger.getLogger(this.getClass());
			LOG.error("IO error while filtering tweets", ex);
			LOG.trace(null, ex);
		}
		return null;
	}
}
