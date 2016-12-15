package data.yunsom.com.service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.alibaba.fastjson.JSONArray;

import data.yunsom.com.format.categoryThreeUrl;
import data.yunsom.com.util.DbUtils;
import data.yunsom.com.util.JsonValidatorUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class CategoryThree {
	private static final Logger logger = LoggerFactory
			.getLogger(CategoryThree.class);

	public static void categorySerch() {
		ObjectMapper mapper = new ObjectMapper();
		// 查询表spider_commodity所有tag_id
		List<Map<String, String>> rs = new ArrayList<Map<String, String>>();
		String url = "www.ehsy.com";
		String sql = "select tag_id from spider_commodity  where `from` =  '"
				+ url + "' group by tag_id ";
		rs = DbUtils.execute(sql);
		HashSet<Integer> tagset = new HashSet<Integer>();
		for (Map<String, String> map : rs) {
			tagset.add(Integer.parseInt(map.get("tag_id")));
		}
		for (Integer tag_id : tagset) {
			String sql_group = "select * from spider_commodity  where `from` =  '"
					+ url + "' and tag_id =  " + tag_id;
			rs = DbUtils.execute(sql_group);

			for (Map<String, String> map : rs) {
				// String tag_id = map.get("tag_id");
				// 查询表comd_tag_meta所有tag_id的值
				String tag_name = searchTagMeta(tag_id + "");
				String commodity_name = map.get("commodity_name");
				String id = map.get("id");
				String commodity_model = map.get("commodity_model");
				String brand_id = map.get("brand_id");
				// 查询表comd_brand所有brand_id的值
				String brand_name = searchComdBrand(brand_id);
				String attr = map.get("commodity_attr");

				// 逗号链接之后的commodity_att为commodity_attr
				StringBuffer commodity_attr = new StringBuffer();
				if (!"".equals(attr) && attr != null) {
					boolean validity = new JsonValidatorUtil().validate(attr);
					if (validity) {
						JSONArray array = JSONArray.parseArray(attr);
						int arrsize = array.size();
						for (int i = 0; i < arrsize; i++) {

							String attrKey = array.getJSONObject(i)
									.get("attrkey").toString();
							String attrValue = array.getJSONObject(i)
									.get("keyname").toString();
							commodity_attr.append(attrKey + "," + attrValue
									+ ",");

						}
					}
				}
				// 所有数据json存入
				categoryThreeUrl categorythreeUrl = new categoryThreeUrl(
						commodity_name, commodity_attr, tag_name, brand_name,
						commodity_model);
				try {
					String json = mapper.writeValueAsString(categorythreeUrl);
					System.out.println("json" + json);
					logger.info("json--" + json);
					/* 以tag_name写文件 */
					// System.out.println("tag_name:"+tag_name);
					// System.out.println("id:"+id);
					if (tag_name != (null)) {
						tag_name = tag_name.replace("/", "").replace("*", "");
						writeFileTag(tag_name, json);
					}
					// tag_name空时候，以tag_id为文件名写文件
					else {
						writeFileTag(tag_id + "", json);
					}

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

	private static void writeFileTag(String tag_name, String json)
			throws IOException {
		File file_one = new File("public/categoryTag/"+tag_name+".json");
		FileWriter fw_one = new FileWriter(file_one.getAbsoluteFile(),true);
		BufferedWriter bw_one =null;
		bw_one = new BufferedWriter(fw_one);
		bw_one.append(json);
		bw_one.close();
	}

	// 查询表comd_brand
	private static String searchComdBrand(String brand_id) {
		List<Map<String, String>> rs = new ArrayList<Map<String, String>>();
		String sql = "select * from comd_brand where id = '" + brand_id + "' ";
		rs = DbUtils.execute(sql);
		for (Map<String, String> map : rs) {
			String brand_name = map.get("brand_name");
			return brand_name;

		}

		return null;
	}

	// 查询表comd_tag_meta
	private static String searchTagMeta(String tag_id2) {
		List<Map<String, String>> rs = new ArrayList<Map<String, String>>();
		String sql = "select * from comd_tag_meta where id = '" + tag_id2
				+ "' ";
		rs = DbUtils.execute(sql);
		for (Map<String, String> map : rs) {
			String tag_id = map.get("tag_id");
			// 查询表comd_tag所有tag_id的值
			String tag_id_value = searchTag(tag_id);
			return tag_id_value;

		}

		return null;
	}

	// 查询表comd_tag
	private static String searchTag(String tag_id2) {
		List<Map<String, String>> rs = new ArrayList<Map<String, String>>();
		String sql = "select * from comd_tag where id='" + tag_id2 + "' ";

		rs = DbUtils.execute(sql);
		for (Map<String, String> map : rs) {
			String tag_value = map.get("tag_name");
			return tag_value;

		}
		return null;
	}

}
