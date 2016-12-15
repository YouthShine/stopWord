package data.yunsom.com.service;



import java.util.List;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import data.yunsom.com.common.Config;
import data.yunsom.com.util.DbUtils;
import data.yunsom.com.util.ElasticsearchUtil;
import data.yunsom.com.util.SuggestPinyinDataUtil;

public class SynchPinyinData {

	private static final Logger logger = LoggerFactory
			.getLogger(SynchPinyinData.class);
	public static TransportClient client = ElasticsearchUtil.getTransportClient();

	/**
	 * 写入分类拼音数据
	 * 
	 * **/
	public static void pushCategoryData()  {

		BulkRequestBuilder bulkRequest = client.prepareBulk();

		String sql = "select id,category_name   from comd_category ";
		List<Map<String, String>> rs = DbUtils.execute(sql);
		for (Map<String, String> map : rs) {
			String category_name = map.get("category_name");

			String pinyinjson = SuggestPinyinDataUtil.getSuggestPinyin(
					category_name, Config.TYPE_WEIGHT_CATE);
			logger.info(pinyinjson);
			bulkRequest.add(client.prepareIndex(Config.SUGGEST,
					Config.TYPE, "category_" + map.get("id"))
					.setSource(pinyinjson));
		}

		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			logger.info(bulkResponse.buildFailureMessage());
		}

	}

	/**
	 * 写入品牌拼音数据
	 * 
	 * **/
	public static void pushBrandData()  {

		BulkRequestBuilder bulkRequest = client.prepareBulk();

		String sql = "select id,brand_name   from comd_brand  ";
		List<Map<String, String>> rs = DbUtils.execute(sql);
		for (Map<String, String> map : rs) {
			String brand_name = map.get("brand_name");
			String pinyinjson = SuggestPinyinDataUtil.getSuggestPinyin(brand_name,
					Config.TYPE_WEIGHT_BRAND);
			logger.info(pinyinjson);

			bulkRequest.add(client.prepareIndex(Config.SUGGEST,
					Config.TYPE, "brand_" + map.get("id"))
					.setSource(pinyinjson));

		}
		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			logger.info(bulkResponse.buildFailureMessage());
		}

	}

	/**
	 * 写入企业拼音数据
	 * 
	 * **/
	public static void pushEnterpriseData() {

		BulkRequestBuilder bulkRequest = client.prepareBulk();

		String sql = "select id,enterprise_name   from uc_enterprise  ";
		List<Map<String, String>> rs = DbUtils.execute(sql);
		for (Map<String, String> map : rs) {
			String enterprise_name = map.get("enterprise_name");

			String pinyinjson = SuggestPinyinDataUtil.getSuggestPinyin(
					enterprise_name, Config.TYPE_WEIGHT_ENTERPRISE);
			logger.info(pinyinjson);
			bulkRequest.add(client
					.prepareIndex(Config.SUGGEST,
							Config.TYPE,
							"enterprise_" + map.get("id"))
					.setSource(pinyinjson));
		}

		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			logger.info(bulkResponse.buildFailureMessage());
		}

	}
}
