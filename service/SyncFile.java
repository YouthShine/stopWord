package data.yunsom.com.service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;

import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.annotate.JsonMethod;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import data.yunsom.com.format.BrandDetail;
import data.yunsom.com.format.BrandInterfaceData;
import data.yunsom.com.format.CommodityUrl;
import data.yunsom.com.format.CommodityValue;
import data.yunsom.com.util.DbUtils;
import data.yunsom.com.util.FileOperate;
import data.yunsom.com.util.JsonValidatorUtil;

public class SyncFile {
	private static final Logger logger = LoggerFactory
			.getLogger(SynchRedis.class);
	// public static TransportClient client = ElasticsearchUtil
	// .getTransportClient();
	private static final FileOperate filecate = new FileOperate();

	public static void fileToElastic(int day) throws IOException {
		HashMap<String, String> webdata = getInsert();
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, -1);
		String yesterday = new SimpleDateFormat("yyyyMMdd").format(cal
				.getTime());
		if(day!=0){
			yesterday=day+"";
		}
		for (String key : webdata.keySet()) {
			// System.out.println("key= "+ key + " and value= " + map.get(key));

			List<String> datas = filecate.readTxt(
					"/root/public/success/"+yesterday+"/"
							+ key + ".json", "utf-8");
			logger.info("data--"+yesterday);
			logger.info("/root/public/success/"+yesterday+"/"
							+ key + ".json");
		//	BulkRequestBuilder bulkRequest = client.prepareBulk();
			ObjectMapper objectMapper = new ObjectMapper();
		
			int num = datas.size();
			if (num > 0) {
				for (int i = 0; i < num; i++) {
					boolean validity = new JsonValidatorUtil().validate(datas
							.get(i));
					if (validity) {
						CommodityUrl commodityUrl = objectMapper.readValue(
								datas.get(i), CommodityUrl.class);
						String url = commodityUrl.getCommodity_url();

						String[] webname = webdata.get(key).split("_");
						commodityUrl.setProvider_id(Integer
								.parseInt(webname[1]));
						commodityUrl.setIs_recommend(true);
						commodityUrl.setProvider_name(webname[0]);
						commodityUrl.setPlatform_name(webname[0]);
						commodityUrl.setPlatform_id(Integer
								.parseInt(webname[1]));
						String json = objectMapper
								.writeValueAsString(commodityUrl);
						//bulkRequest.add(client.prepareIndex(Config.INDEXNAME,
							//	Config.TYPE, url).setSource(json));

					}
				}

				//BulkResponse bulkResponse = bulkRequest.execute().actionGet();
				//if (bulkResponse.hasFailures()) {
					//logger.error(bulkResponse.buildFailureMessage());
				}
			}
		}

	//}
	
	
    /*从public目录下读取所有的json文件*/
	public static void dealTag() throws IOException {
		
			
		//定义一个list,其值为所有json文件名
		List<String> Tagdata = getTagJson();
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, -1);
		String yesterday = new SimpleDateFormat("yyyyMMdd").format(cal
				.getTime());
		//访问接口不存在品牌id时候  将数据写入一个文件，将该id写入另外一个文件  
		File file_one = new File("./public/brand/new_brandData.json");
		  FileWriter fw_one = new FileWriter(file_one.getAbsoluteFile(),true);
		  File file_two = new File("./public/brand/new_brand.txt");
		  FileWriter fw_two = new FileWriter(file_two.getAbsoluteFile(),true);
		  BufferedWriter bw_one =null;
		  BufferedWriter bw_two =null;
		  
		for (String value : Tagdata) {	
		List<String> datas = filecate.readTxt("./public/json"+"/"+value, "utf-8");
		logger.info("data--" + yesterday);
		logger.info("./public/success/" + yesterday + "/" + "ehsy" + ".json");
		// BulkRequestBuilder bulkRequest = client.prepareBulk();
		ObjectMapper objectMapper = new ObjectMapper();	
		int num = datas.size();
		System.out.println(num+"pppp66666666666");
		if (num > 0) {	
			
			for (int i = 0; i < num; i++) {	
				//判断是否为合格json串
				boolean validity = new JsonValidatorUtil().validate(datas
						.get(i));
				// System.out.println(datas.get(i));
				if (validity && !datas.get(i).equals("")) {
					logger.info(datas.get(i)+"-----");
					CommodityValue commodityValue = objectMapper.readValue(
							datas.get(i), CommodityValue.class);
					/*通过读取的json串  取相应的值*/
					String url = commodityValue.getCommodity_url();
					String category_name_one = commodityValue
							.getCategory_name_one();
					String category_name_two = commodityValue
							.getCategory_name_two();
					String category_name = commodityValue
							.getCategory_name();
					String brand_name = commodityValue
							.getBrand_name();
					String commodity_name = commodityValue.getCommodity_name().replace("\r\n", "").replace("\t", "").replace("\b", "");
					List commodity_model_l = commodityValue.getCommodity_attr();
					ObjectMapper mapper = new ObjectMapper();
					mapper.setVisibility(JsonMethod.FIELD, Visibility.ANY);
					mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
					String json = mapper
							 .writeValueAsString(commodity_model_l);
					String	commodity_model = json.substring(json.indexOf("attrname") + 8,json.indexOf("}")).replace("\"", "").replace("}", "").replace(":", "").replace("\r\n", "").replace("\t", "").replace("\b", "");
										
					String commodity_url = commodityValue.getCommodity_url();
					int tag_id_meta = 0;
					List commodity_attr_l = commodityValue.getSpeci();
					String json_one = mapper
							 .writeValueAsString(commodity_attr_l);
					String	commodity_attr = json_one.replace("\r\n", "").replace("\t", "").replace("\b", "");
					List commodity_introduce_l = commodityValue.getPack();
					String json_two = mapper
							 .writeValueAsString(commodity_attr_l);
					String	commodity_introduce = json_two.replace("\r\n", "").replace("\t", "").replace("\b", "");
					String main_pic_url = commodityValue.getMain_pic_url().replace("\r\n", "").replace("\t", "").replace("\b", "");
					double price = commodityValue.getPrice();
					String from_l = commodityValue.getCommodity_url().replace("\r\n", "").replace("\t", "").replace("\b", "");
					java.net.URL urls = new java.net.URL(from_l);
					String from = urls.getHost().replace("\r\n", "").replace("\t", "").replace("\b", "");
					//通过查询表comd_tadg 若返回的id等于0   说明该分类不存在表中  故插入该分类 ， 并将出入后返回的id（3个分类id的索引）插入表comd_tag_mete
					int tag_id_one=0;
					int tag_id_two=0;
					int tag_id = 0;
					String tag_id_one_s = null;
					String tag_id_two_s = null;
					String tag_id_s = null;
					String tag_id_meta_s = null;
					int tag_id_spider = 0;
					int code = 1;
					int brand_id=0;
					String tag_id_spider_s = null;
					//comd_tadg
					if(category_name_one!=null){
					String sql = "select id from comd_tag where tag_name = '"+category_name_one+"'";
					//tag_id_one = DbUtils.executeId(sql);
					tag_id_one_s = Integer.toString(tag_id_one);
					logger.info("tag_id_one_s:"+tag_id_one_s);
					if(tag_id_one_s.equals("0")){
						String sql_insert = " insert into comd_tag  (tag_name) values(?)";
						tag_id_one=DbUtils.insert(sql_insert,category_name_one);
						logger.info("tag_id_one:"+tag_id_one);
					}
					}
					if(category_name_two!=null){
						String sql = "select id from comd_tag where tag_name = '"+category_name_two+"'";
						//tag_id_two = DbUtils.executeId(sql);
						tag_id_two_s = Integer.toString(tag_id_two);
						logger.info("tag_id_two_s:"+tag_id_two_s);
						if(tag_id_two_s.equals("0")){
							String sql_insert = " insert into comd_tag  (tag_name) values(?)";
							tag_id_two = DbUtils.insert(sql_insert,category_name_two);
							logger.info("tag_id_two:"+tag_id_two);
						}
						}
					if(category_name!=null){
						String sql = "select id from comd_tag where tag_name = '"+category_name+"'";
						//tag_id = DbUtils.executeId(sql);
						tag_id_s = Integer.toString(tag_id);
						logger.info("tag_id_two_s:"+tag_id_s);
						if(tag_id_s.equals("0")){
							String sql_insert = "insert into comd_tag  (tag_name) values(?)";
							tag_id = DbUtils.insert(sql_insert,category_name);
							logger.info("tag_id:"+tag_id);
						}
						}
					//查询表comd_tag_mete判断3个id都不在该表中时候，  插入该索引id
					String sql_meta = "select id from comd_tag_meta  where tag_id_one = '"+tag_id_one_s+"' and tag_id_two = '"+tag_id_two_s+"' and tag_id = '"+tag_id_s+"'";
					tag_id_meta = DbUtils.executeIdd(sql_meta);
					tag_id_meta_s = Integer.toString(tag_id_meta);
					
					if(tag_id_meta_s.equals("0")){
						
						String sql_insert = " insert into comd_tag_meta  (tag_id_one,tag_id_two,tag_id) values(?,?,?)";
						tag_id_meta = DbUtils.insert_meta(sql_insert,tag_id_one,tag_id_two,tag_id);
						tag_id_meta_s = Integer.toString(tag_id_meta);
						logger.info("tag_id_meta_s:"+tag_id_meta_s);
						//System.out.println(tag_id_meta+"ooooo55555555555959546");
					}
					
					//通过访问接口interface，判断品牌id是否在，不存在吧品牌ID写入一个文件，该条json数据写入另外一个文件
					String brand_name_u = null;
					try {
						brand_name_u = URLEncoder.encode(brand_name, "utf-8");
					} catch (UnsupportedEncodingException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					String brand_url = "http://192.168.1.233:8080/es/spider/matchBrand?keyword=" + brand_name_u;
					//将返回的数据存入list
					List<String> brand_interface = new ArrayList<String>();
					try {
						URL urlObject = new URL(brand_url);
						URLConnection uc = urlObject.openConnection();
						BufferedReader in = new BufferedReader(new InputStreamReader(
								uc.getInputStream(), "utf-8"));
						String inputLine = null;
						while ((inputLine = in.readLine()) != null) {
							brand_interface.add(inputLine);
						}
						in.close();
					} catch (MalformedURLException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
					int num_interface = brand_interface.size();
					 List<BrandDetail> brand_id_l =  new ArrayList<BrandDetail>();
					if (num_interface > 0) {
						    //判断返回品牌数据是否是json串（仅仅一条）
							boolean validity_interface = new JsonValidatorUtil().validate(brand_interface
									.get(0));
							if (validity_interface) {
								//通过json对象映射相应的值
								BrandInterfaceData brandInterfaceData = objectMapper.readValue(
										brand_interface.get(0), BrandInterfaceData.class);
								//getData读取品牌的相关值（也是一个list）
								brand_id_l = brandInterfaceData.getData();
								ObjectMapper mapper_one = new ObjectMapper();
								//取出品牌id ,mapper_one封装json的方法
								mapper_one.setVisibility(JsonMethod.FIELD, Visibility.ANY);
								mapper_one.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
								//brand_id_l（list,json）转为字符串
								String json_brand = mapper_one
										 .writeValueAsString(brand_id_l);
								if(brand_id_l.size()>0){
								String	brand_id_s = json_brand.substring(json_brand.indexOf("brand_id") + 8,json_brand.indexOf(",")).replace("\"", "").replace("}", "").replace(":", "").replace(" ", "");
								logger.info("brind_id:"+brand_id_s);
								brand_id = Integer.parseInt(brand_id_s);
								
								}
								//取出品牌code
								code = brandInterfaceData.getCode();
								
					}
						
						}
					//brand not exit,write file
					if(code==1){
						 if (!file_one.exists()) {
							 file_one.createNewFile();
							   }
						 if (!file_two.exists()) {
							 file_two.createNewFile();
							   }

							  
							   bw_one = new BufferedWriter(fw_one);
							   bw_one.append(datas.get(i)+"\r\n");
							   bw_two = new BufferedWriter(fw_two);
							   bw_two.append(brand_name+"\r\n");
							 
					}
					else{
					//table@spider_commodity
					String sql_spider = "select id from spider_commodity  where commodity_url = '"+commodity_url+"'";
					tag_id_spider = DbUtils.executeIdd_spider(sql_spider);
					tag_id_spider_s = Integer.toString(tag_id_spider);
					logger.info("tag_id_spider_s:"+tag_id_spider_s);
					if(tag_id_spider_s.equals("0")){
					String sql_insert = " insert into spider_commodity  (commodity_name,commodity_model,commodity_url,tag_id,brand_id,commodity_attr,commodity_introduce,main_pic_url,price,`from`) values(?,?,?,?,?,?,?,?,?,?)";
					tag_id_spider = DbUtils.insert_spider(sql_insert,commodity_name,commodity_model,commodity_url,tag_id,brand_id,commodity_attr,commodity_introduce,main_pic_url,price,from);
					tag_id_spider_s = Integer.toString(tag_id_meta);
					
					}
					}
					
					logger.info("url:"+url);
					logger.info("category_name_one:"+category_name_one);
					logger.info("category_name_two:"+category_name_two);
					logger.info("category_name:"+category_name);
					logger.info("brand_name:"+brand_name);
					

					

				}
			}
			
           
		}
		
		}
		
		bw_one.close();
		bw_two.close();
	}

	public static HashMap<String, String> getInsert() {

		HashMap<String, String> data = new HashMap<String, String>();
		data.put("315mro", "传奇商城_100000");
		data.put("4006770558", "网尚购_100001");
		data.put("91yilong", "易隆商城_100002");
		data.put("btone-mro", "丙通MRO_100003");
		data.put("deppre", "德普瑞工业商城_100004");
		data.put("gongchang", "工厂易购_100005");
		data.put("ispek", "思贝壳_100006");
		data.put("isweek", "工采网_100007");
		data.put("mrobay", "陌贝网_100008");
		data.put("makepolo", "马可波罗_100009");
		data.put("rolymro", "苏州雷利_100010");
		data.put("seton", "赛盾_100011");
		data.put("zkh360", "震坤行工业超市_100012");
		data.put("ehsy", "西域_100013");
		data.put("haocaimao", "好采猫_100014");
		data.put("8shop", "都工业网_100015");
		data.put("mctmall", "中国工量具商城_100016");
		data.put("grainger", "固安捷_100017");
		data.put("wwmro", "土狼_100018");
		data.put("iacmall", "艾驰商城_100019");
		data.put("vipmro", "工品汇_100020");
		data.put("1ez", "一站工材_100021");
		data.put("axmro", "艾逊_100022");
		data.put("gomro", "固买网_100023");
		data.put("hc360", "慧聪网_100024");
		data.put("zgw", "中钢网_100025");
		data.put("huaaomro", "华澳MRO商城_100026");
		return data;
	}
	public static List<String> getTagJson() {

		List<String> dataList = new ArrayList<String>();
		dataList.add("315mro.json");
		dataList.add("4006770558.json");
		dataList.add("91yilong.json");
		dataList.add("btone-mro.json");
		dataList.add("deppre.json");
		dataList.add("gongchang.json");
		dataList.add("ispek.json");
		dataList.add("isweek.json");
		dataList.add("mrobay.json");
		dataList.add("makepolo.json");
		dataList.add("rolymro.json");
		dataList.add("seton.json");
		dataList.add("zkh360.json");
		dataList.add("ehsy.json");
		dataList.add("haocaimao.json");
		dataList.add("8shop.json");
		dataList.add("mctmall.json");
		dataList.add("grainger.json");
		dataList.add("wwmro.json");
		dataList.add("iacmall.json");
		dataList.add("vipmro.json");
		dataList.add("1ez.json");
		dataList.add("axmro.json");
		dataList.add("gomro.json");
		dataList.add("hc360.json");
		dataList.add("zgw.json");
		dataList.add("huaaomro.json");
		dataList.add("zhaogongye.json");
		return dataList;
	}

}
