package com.shk.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class StatsCounterTuple implements Writable{

	private int usedCounter;
	private int newCounter;
	private int successCounter;
	private int failedCounter;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		usedCounter = in.readInt();
		newCounter = in.readInt();
		successCounter = in.readInt();
		failedCounter = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(usedCounter);
		out.writeInt(newCounter);
		out.writeInt(successCounter);
		out.writeInt(failedCounter);
		
	}

	public StatsCounterTuple() {
	}
	
	public StatsCounterTuple(int usedCounter, int newCounter,
			int successCounter, int failedCounter) {
		this.usedCounter = usedCounter;
		this.newCounter = newCounter;
		this.successCounter = successCounter;
		this.failedCounter = failedCounter;
	}

	@Override
	public String toString() {
		return usedCounter + " " + newCounter + " " + successCounter + " " + failedCounter;
	}

	public int getUsedCounter() {
		return usedCounter;
	}

	public void setUsedCounter(int usedCounter) {
		this.usedCounter = usedCounter;
	}

	public int getNewCounter() {
		return newCounter;
	}

	public void setNewCounter(int newCounter) {
		this.newCounter = newCounter;
	}

	public int getSuccessCounter() {
		return successCounter;
	}

	public void setSuccessCounter(int successCounter) {
		this.successCounter = successCounter;
	}

	public int getFailedCounter() {
		return failedCounter;
	}

	public void setFailedCounter(int failedCounter) {
		this.failedCounter = failedCounter;
	}
	
	public void add(StatsCounterTuple counter) {
		this.usedCounter += counter.usedCounter;
		this.newCounter += counter.newCounter;
		this.successCounter += counter.successCounter;
		this.failedCounter += counter.failedCounter;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + failedCounter;
		result = prime * result + newCounter;
		result = prime * result + successCounter;
		result = prime * result + usedCounter;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StatsCounterTuple other = (StatsCounterTuple) obj;
		if (failedCounter != other.failedCounter)
			return false;
		if (newCounter != other.newCounter)
			return false;
		if (successCounter != other.successCounter)
			return false;
		if (usedCounter != other.usedCounter)
			return false;
		return true;
	}
}
