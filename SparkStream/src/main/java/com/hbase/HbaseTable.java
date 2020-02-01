package com.hbase;


public class HbaseTable {

	private static final String TABLE_NAME = "tweet";
	private static final String CF_DETAIL = "tweet_detail";
/**
	static Configuration config = HBaseConfiguration.create();

	public void createTweetTable() throws IOException {

		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin()) {
			HTableDescriptor table = new HTableDescriptor(
					TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor(CF_DETAIL)
					.setCompressionType(Algorithm.NONE));
			table.addFamily(new HColumnDescriptor(CF_DETAIL));

			System.out.print("Creating table.... ");

			if (admin.tableExists(table.getTableName())) {
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);

			System.out.println(" Done!");
		}
	}

	public void insertTweet(Configuration config, ClntTweet clntTweet)
			throws IOException {

		DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
		String strDate = dateFormat.format(clntTweet.getCreatedAt());

		HTable hTable = new HTable(config, TABLE_NAME);
		Put put = new Put(Bytes.toBytes(clntTweet.getId()));
		put.addColumn(Bytes.toBytes(CF_DETAIL), Bytes.toBytes("Created At"),
				Bytes.toBytes(strDate));
		put.addColumn(Bytes.toBytes(CF_DETAIL), Bytes.toBytes("Username"),
				Bytes.toBytes(clntTweet.getUsername()));
		put.addColumn(Bytes.toBytes(CF_DETAIL), Bytes.toBytes("Tweet Content"),
				Bytes.toBytes(clntTweet.getTweetContent()));
		put.addColumn(Bytes.toBytes(CF_DETAIL), Bytes.toBytes("Hashtags"),
				Bytes.toBytes(clntTweet.getHashTags()));
		hTable.put(put);

		hTable.close();
	}
*/	
}