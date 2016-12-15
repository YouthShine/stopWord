package data.yunsom.com.service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.http.client.ClientProtocolException;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;

import data.yunsom.com.format.categoryThreeUrl;
import data.yunsom.com.util.DbUtils;
import data.yunsom.com.util.JsonValidatorUtil;


public class CommodityAttrSearch {
	private static final Logger logger = LoggerFactory
			.getLogger(CategoryThree.class);

	public static void attrSerch() throws ClientProtocolException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		// 查询表spider_commodity所有tag_id
		List<Map<String, String>> rs = new ArrayList<Map<String, String>>();
		String sql = "select tag_id from spider_commodity group by tag_id ";
		rs = DbUtils.execute(sql);
		HashSet<Integer> tagset = new HashSet<Integer>();
		for (Map<String, String> map : rs) {
			tagset.add(Integer.parseInt(map.get("tag_id")));
		}
		for (Integer tag_id : tagset) {
			String sql_group = "select * from spider_commodity  where tag_id =  '"+tag_id+"'";
			rs = DbUtils.execute(sql_group);

			for (Map<String, String> map : rs) {				
				// 查询表comd_tag_meta所有tag_id的值
				String attr = map.get("commodity_attr");
				if (!"".equals(attr) && attr != null) {
					boolean validity = new JsonValidatorUtil().validate(attr);
					if (validity) {
						JSONArray array = JSONArray.parseArray(attr);
						int arrsize = array.size();
						for (int i = 0; i < arrsize; i++) {
							//调用接口之后分词结果key_result
							List<String> key_result = new ArrayList<String>();
							List<String> value_result = new ArrayList<String>();
							String attrKey = array.getJSONObject(i)
									.get("attrkey").toString();
							String attrValue = array.getJSONObject(i)
									.get("keyname").toString();
							//调用接口  进行分词
							key_result = HttpClientTest.getData(attrKey);
							value_result = HttpClientTest.getData(attrValue);
							try {
								/* 把分词结果写到文件中 */
								// System.out.println("tag_name:"+tag_name);
								// System.out.println("id:"+id);
								String file_name = "commodityAttrs.txt";
								writeFileTag(file_name,key_result);
								writeFileTag(file_name,value_result);
						
						

							} catch (JsonGenerationException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
								String errors = e.toString();
								logger.error(errors);
							} catch (JsonMappingException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
								String errors = e.toString();
								logger.error(errors);
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
								String errors = e.toString();
								logger.error(errors);
							}
							

						}
					}
				}
				
		
			}
		}

	
			}

	private static void writeFileTag(String fileName, List<String> key_result)
			throws IOException {
		File file_one = new File("./public/commodityAttr/"+fileName);
		if (!file_one.exists()) {
			 file_one.createNewFile();}
		FileWriter fw_one = new FileWriter(file_one.getAbsoluteFile(),true);
		BufferedWriter bw_one =null;
		bw_one = new BufferedWriter(fw_one);
		for (String string : key_result) {
			//System.out.println("attr_result:"+string);
			logger.info("attr_result:"+string);
			bw_one.append(string+"\r\n");
			
		}
		
		bw_one.close();
	}
}
