package data.yunsom.com.util;


public class Token {
	public Token(){
		
	}
	private String token;
	public String getToken() {
		return token;
	}
	public Token(String token, int start_offset, int end_offset, String type,
			int position) {
		
		this.token = token;
		this.start_offset = start_offset;
		this.end_offset = end_offset;
		this.type = type;
		this.position = position;
	}
	public void setToken(String token) {
		this.token = token;
	}
	public int getStart_offset() {
		return start_offset;
	}
	public void setStart_offset(int start_offset) {
		this.start_offset = start_offset;
	}
	public int getEnd_offset() {
		return end_offset;
	}
	public void setEnd_offset(int end_offset) {
		this.end_offset = end_offset;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public int getPosition() {
		return position;
	}
	public void setPosition(int position) {
		this.position = position;
	}
	private int start_offset ;
	private int end_offset ;
	private String type;
	private int position;

}

