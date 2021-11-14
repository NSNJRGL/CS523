package cs523.spark_sql;

public class TweetInfo {
	
	private String userName;
	private Long followersCount;
	
	public void initialize(String userName, Long followersCount) {
		this.userName = userName;
		this.followersCount = followersCount;
	}
}
