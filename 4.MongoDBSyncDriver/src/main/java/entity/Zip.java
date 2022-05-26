package entity;

import java.util.Arrays;

public class Zip {
	private String _id;
	private String city;
	private double[] loc = new double[2];
	private int pop;
	private String state;
	
	public Zip() {
	}
	
	public Zip(String _id, String city, double[] loc, int pop, String state) {
		super();
		this._id = _id;
		this.city = city;
		this.loc = loc;
		this.pop = pop;
		this.state = state;
	}
	public String get_id() {
		return _id;
	}
	public void set_id(String _id) {
		this._id = _id;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public double[] getLoc() {
		return loc;
	}
	public void setLoc(double[] loc) {
		this.loc = loc;
	}
	public int getPop() {
		return pop;
	}
	public void setPop(int pop) {
		this.pop = pop;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	@Override
	public String toString() {
		return "Zip [_id=" + _id + ", city=" + city + ", loc=" + Arrays.toString(loc) + ", pop=" + pop + ", state="
				+ state + "]";
	}
	
	
	
}
