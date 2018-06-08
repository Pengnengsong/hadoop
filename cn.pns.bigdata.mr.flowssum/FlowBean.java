
package cn.pns.bigdata.mr.flowssum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class FlowBean implements WritableComparable<FlowBean>{

	
	private long upFlow; // 上行流量
	private long dFlow;  // 下行流量
	private long totalFlow; // 总流量
	
	// 反序列化时，需要反射调用空参构造函数，要显示定义一个
	public FlowBean() {
		super();
	}

	public FlowBean(long upFlow, long dFlow) {
		this.upFlow = upFlow;
		this.dFlow = dFlow;
		this.totalFlow = upFlow + dFlow;
	}
	
	public void setFlowBean(long upFlow, long dFlow) {
		this.upFlow = upFlow;
		this.dFlow = dFlow;
		this.totalFlow = upFlow + dFlow;
	}
	
	public long getUpFlow() {
		return upFlow;
	}
	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}
	public long getdFlow() {
		return dFlow;
	}
	public void setdFlow(long dFlow) {
		this.dFlow = dFlow;
	}

	
	public long getTotalFlow() {
		return totalFlow;
	}

	public void setTotalFlow(long totalFlow) {
		this.totalFlow = totalFlow;
	}

	/**
	 * 序列化
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(dFlow);
		out.writeLong(totalFlow);
	}
	

	/**
	 * 反序列化
	 * 注意：顺序一致，先进先出
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		upFlow = in.readLong();
		dFlow = in.readLong();
		totalFlow = in.readLong();
	}

	@Override
	public String toString() {
		 
		return upFlow + "\t" + dFlow + "\t" + totalFlow;
	}

	@Override
	public int compareTo(FlowBean o) {
		
		return this.totalFlow>o.getTotalFlow()?-1:1;
	}
	
	
}

