package com.sparktesting.model;

import java.io.Serializable;

public class TweetData implements Serializable {
	
	
	private String tweet;
	
	private int score;
	
	private String lang;
	
	private String json;
	
	

	public String getJson() {
		return json;
	}

	public void setJson(String json) {
		this.json = json;
	}

	public String getTweet() {
		return tweet;
	}

	public void setTweet(String tweet) {
		this.tweet = tweet;
	}

	

	public int getScore() {
		return score;
	}

	public void setScore(int score) {
		this.score = score;
	}

	public String getLang() {
		return lang;
	}

	public void setLang(String lang) {
		this.lang = lang;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		String result = getTweet() + "," +getScore(); 
		return result;
	}

}
