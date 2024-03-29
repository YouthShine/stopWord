package data.yunsom.com.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.annotate.JsonMethod;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import data.yunsom.com.common.Config;
import data.yunsom.com.format.BrandMapping;
import data.yunsom.com.util.DbUtils;
import data.yunsom.com.util.ElasticsearchUtil;

/*
 * 新增品牌INDEX
 * **/
public class SynchBrand {

	private static final Logger logger = LoggerFactory
			.getLogger(SynchBrand.class);
	public static TransportClient client = ElasticsearchUtil.getTransportClient();
	
	public static void insertBrand() throws JsonGenerationException, JsonMappingException, IOException{
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		ObjectMapper mapper = new ObjectMapper();
		mapper.setVisibility(JsonMethod.FIELD, Visibility.ANY);
		mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
		String sql = "select id,brand_name from comd_brand";
		List<Map<String, String>> rs = DbUtils.execute(sql);
		for (Map<String, String> map : rs) {
			String brand_name = map.get("brand_name");
			int brand_id =  Integer.parseInt(map.get("id"));
			String brand_name_cn ="";
			String brand_name_en ="";
			if(brand_name.indexOf("/") >0){
				String[] brand_arr = brand_name.split("/");
				brand_name_cn= brand_arr[0];
				brand_name_en = brand_arr[1];
				
			}else{
				 brand_name_cn = brand_name;
				 brand_name_en = brand_name;
			}
			BrandMapping resJson =  new BrandMapping(brand_id, brand_name, brand_name_en, brand_name_cn);
			String json = mapper.writeValueAsString(resJson);
					 bulkRequest.add(client.prepareIndex(
					 Config.INDEXNAMEBRNAD, Config.INDEXNAMEBRNAD,
					 brand_id+"")
					 .setSource(json));
		}
		if (bulkRequest.numberOfActions() > 1) {
			BulkResponse bulkResponse = bulkRequest.execute()
					.actionGet();
			if (bulkResponse.hasFailures()) {
				logger.info(bulkResponse.buildFailureMessage());
			}
		}
	}
	
	
	
}
