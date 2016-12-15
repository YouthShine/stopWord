package data.yunsom.com.format;

import java.util.List;

public class categoryThreeUrl {
	public String getCommodity_name() {
		return commodity_name;
	}
	public void setCommodity_name(String commodity_name) {
		this.commodity_name = commodity_name;
	}
	public String getCommodity_model() {
		return commodity_model;
	}
	public void setCommodity_model(String commodity_model) {
		this.commodity_model = commodity_model;
	}
	public String getTag_name() {
		return tag_name;
	}
	public void setTag_name(String tag_name) {
		this.tag_name = tag_name;
	}
	public String getBrand_name() {
		return brand_name;
	}
	public void setBrand_name(String brand_name) {
		this.brand_name = brand_name;
	}
	public StringBuffer getCommodity_attr() {
		return commodity_attr;
	}
	public void setCommodity_attr(StringBuffer commodity_attr) {
		this.commodity_attr = commodity_attr;
	}
	private String commodity_name;
	private String commodity_model;
	private String tag_name;
	private String brand_name;
	private StringBuffer commodity_attr;
	public categoryThreeUrl(){}
	public categoryThreeUrl(String commodity_name,
			StringBuffer commodity_attr, String tag_name,
			String brand_name, String commodity_model) {
		
		super();
		this.commodity_name = commodity_name;
		this.commodity_model = commodity_model;
		this.tag_name = tag_name;
		this.brand_name = brand_name;
		this.commodity_attr = commodity_attr;
	}
	

}
