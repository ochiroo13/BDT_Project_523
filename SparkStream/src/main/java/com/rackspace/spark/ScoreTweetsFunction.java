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

import org.apache.spark.api.java.function.Function;

import scala.Tuple4;
import scala.Tuple5;

public class ScoreTweetsFunction
		implements
		Function<Tuple4<Long, String, Float, Float>, Tuple5<Long, String, Float, Float, String>> {
	private static final long serialVersionUID = 42l;

	@Override
	public Tuple5<Long, String, Float, Float, String> call(
			Tuple4<Long, String, Float, Float> tweet) {
		String score;
		if (tweet._3() >= tweet._4())
			score = "positive";
		else
			score = "negative";
		return new Tuple5<Long, String, Float, Float, String>(tweet._1(),
				tweet._2(), tweet._3(), tweet._4(), score);
	}

}
