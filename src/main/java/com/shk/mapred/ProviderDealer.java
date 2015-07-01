package com.shk.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ProviderDealer implements WritableComparable<ProviderDealer>{

	private Text providerId;
	private Text dealerId;
	@Override
	public void readFields(DataInput in) throws IOException {
		providerId.readFields(in);
		dealerId.readFields(in);
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		providerId.write(out);
		dealerId.write(out);
	}
	@Override
	public int compareTo(ProviderDealer providerDealer) {
		if(providerDealer == null) {
			return 0;
		}
		
		int comp = providerId.compareTo(providerDealer.getProviderId());
		if(comp == 0) {
			return dealerId.compareTo(providerDealer.getDealerId());
		} else {
			return comp;
		}
	}
	public ProviderDealer() {
		this.providerId = new Text();
		this.dealerId = new Text();
	}
	public ProviderDealer(Text providerId, Text dealerId) {
		this.providerId = providerId;
		this.dealerId = dealerId;
	}
	public Text getProviderId() {
		return providerId;
	}
	public Text getDealerId() {
		return dealerId;
	}
	@Override
	public String toString() {
		return providerId + " " + dealerId;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((dealerId == null) ? 0 : dealerId.hashCode());
		result = prime * result
				+ ((providerId == null) ? 0 : providerId.hashCode());
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
		ProviderDealer other = (ProviderDealer) obj;
		if (dealerId == null) {
			if (other.dealerId != null)
				return false;
		} else if (!dealerId.equals(other.dealerId))
			return false;
		if (providerId == null) {
			if (other.providerId != null)
				return false;
		} else if (!providerId.equals(other.providerId))
			return false;
		return true;
	}
	
}
