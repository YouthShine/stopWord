package data.yunsom.com.service;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import com.fasterxml.jackson.databind.ObjectMapper;

import data.yunsom.com.util.Token;
import data.yunsom.com.util.Tokens;

public class HttpClientTest {
	private static HttpClient client = new DefaultHttpClient();
	public static List<String> getData(String keyword) throws ClientProtocolException, IOException
		{
		List<String> keywords = new ArrayList<String>();
		
		keyword = URLEncoder.encode(keyword,"utf-8");
		String url = "http://192.168.1.227:9200/tag/_analyze?analyzer=ik&pretty=true&text="
				+ keyword;
		
		
		HttpGet request = new HttpGet(url);

		HttpResponse response = client.execute(request);

		BufferedReader rd = new BufferedReader(new InputStreamReader(response
				.getEntity().getContent()));
		String line = "";
		String res = "";
		while ((line = rd.readLine()) != null) {
			res += line;
		}
		ObjectMapper objectMapper = new ObjectMapper();
		//System.out.println("77777777"+res);
		if (res.contains("tokens")){
		Tokens tokens = objectMapper.readValue(res, Tokens.class);
		for (Token rs : tokens.getTokens()) {
			if (rs.getEnd_offset() - rs.getStart_offset() > 1) {
				keywords.add(rs.getToken());
			}

		}
		}
		return keywords;
	}

	public static String StringFilter(String str) throws PatternSyntaxException {
		// 只允许字母和数字
		// String regEx = "[^a-zA-Z0-9]";
		// 清除掉所有特殊字符
		String regEx = "[`~!@#$%^&*()+=|{}':;',\\[\\].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？]";
		Pattern p = Pattern.compile(regEx);
		Matcher m = p.matcher(str);
		return m.replaceAll("").trim();
	}

}
