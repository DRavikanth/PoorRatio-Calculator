package org.test.spark.app;

import java.io.Serializable;

public class DataPoint implements Serializable{
	private static final long serialVersionUID = 1L;
	private String ip;
	private String device_type;
	private float score;
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getDevice_type() {
		return device_type;
	}
	public void setDevice_type(String device_type) {
		this.device_type = device_type;
	}
	public float getScore() {
		return score;
	}
	public void setScore(float score) {
		this.score = score;
	}
}
