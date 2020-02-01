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

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import twitter4j.HashtagEntity;
import twitter4j.Status;

public class TwitterFilterFunction implements
		PairFunction<Status, Long, String> {
	private static final long serialVersionUID = 42l;

	private static final DateFormat DATEFORMAT = new SimpleDateFormat(
			"yyyy-mm-dd hh:mm:ss");

	@Override
	public Tuple2<Long, String> call(Status status) {

		try {
			if (status != null && status.getText() != null) {
				long id = status.getId();
				String strDate = DATEFORMAT.format(status.getCreatedAt());
				String username = status.getUser().getScreenName();

				String text = status.getText();
				StringBuilder hashTags = new StringBuilder();

				for (HashtagEntity hash : status.getHashtagEntities()) {
					hashTags.append(hash.getText()).append(",");
				}

				return new Tuple2<Long, String>(id, text);
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
