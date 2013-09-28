package ar.edu.itba.it.bigdata.mapreduce;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import ar.edu.itba.it.bigdata.mapreduce.cancelledFlights.CancelledFlightsMapper;
import ar.edu.itba.it.bigdata.mapreduce.delay.TakeOffDelayMapper;
import ar.edu.itba.it.bigdata.mapreduce.flightHours.FlightHoursMapper;
import ar.edu.itba.it.bigdata.mapreduce.flownMiles.FlownMilesMapper;


public class AppConfig {

	private Class<? extends Mapper<?,?,?,?>> mapper;
	private Class<? extends Reducer<?,?,?,?>> reducer;
	private String inPath;
	private String outPath;
	
	private Map<String, String> extras = new HashMap<String, String>();
	
	public Map<String, String> getExtras() {
		return extras;
	}
	
	public void setPlaneType(String planeType) {
		extras.put("planeType", planeType);
	}
	
	public void setOutPath(String outPath) {
		this.outPath = outPath;
	}
	
	public String getOutPath() {
		return outPath;
	}
	
	public void setCarriersPath(String carriersPath) {
		extras.put("carriersPath", carriersPath);
	}
	
	public void setInPath(String inPath) {
		this.inPath = inPath;
	}
	
	public String getInPath() {
		return inPath;
	}
	
	public void setMapper(Class<? extends Mapper<?, ?, ?, ?>> mapper) {
		this.mapper = mapper;
	}
	
	public Class<? extends Mapper<?, ?, ?, ?>> getMapper() {
		return mapper;
	}
	
	public void setReducer(Class<? extends Reducer<?, ?, ?, ?>> reducer) {
		this.reducer = reducer;
	}
	
	public Class<? extends Reducer<?, ?, ?, ?>> getReducer() {
		return reducer;
	}
	
	public Class<?> getMapOutputKeyClass() {
		return Text.class;
	}
	
	public Class<?> getMapOutputValueClass() {
		if(mapper.equals(CancelledFlightsMapper.class) || mapper.equals(FlownMilesMapper.class)) {
			return IntWritable.class;
		} else if(mapper.equals(FlightHoursMapper.class)) {
			return DoubleWritable.class;
		} else if(mapper.equals(TakeOffDelayMapper.class)) {
			return Text.class;
		}
		
		return null;
	}
	
	public Class<?> getOutputKeyClass() {
		return Text.class;
	}
	
	public Class<?> getOutputValueClass() {
		if(mapper.equals(CancelledFlightsMapper.class)) {
			return IntWritable.class;
		} else if(mapper.equals(FlightHoursMapper.class)) {
			return DoubleWritable.class;
		} else if(mapper.equals(TakeOffDelayMapper.class)) {
			return DoubleWritable.class;
		} else if(mapper.equals(FlownMilesMapper.class)) {
			return LongWritable.class;
		}
		
		return null;
	}
}