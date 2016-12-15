package data.yunsom.com.service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import data.yunsom.com.client.ThreadPoolTest;
import data.yunsom.com.util.DbUtils;
import data.yunsom.com.util.RedisUtil;

public class SynchRedis {
	private static final Logger logger = LoggerFactory
			.getLogger(SynchRedis.class);

	public static void mysqlToRedis() {
		categoryToReids();
		categoryIDToReids();
		categoryNameToReids();
		brandNameToReids();
		categoryIdParentToReids();
		enterpriseToRedis();
		

		
	}

	public static void categoryToReids(){
		String sql = "select a.id,category_name,max(root_step) as type  from comd_category as a,comd_category_meta  as b where  a.id=b.category_id group by a.id";
		RedisUtil  redisutil = new RedisUtil();
		List<Map<String, String>> rs = DbUtils.execute(sql);
		
		for (Map<String, String> maprs : rs) {
			
			redisutil.set("es_"+maprs.get("category_name"), maprs.get("id")+"_"+maprs.get("type"));
			logger.info("key=es_"+maprs.get("category_name")+"--"+maprs.get("id")+"_"+maprs.get("type"));
		}
	}

	/***
	 * 分类ID=》key
	 * 分类级别=>value
	 * */
	public static void categoryIDToReids(){
		RedisUtil  redisutil = new RedisUtil();
		String parent_ids = "";
		String sql = "select id,category_name from comd_category where parent_id=0";
		List<Map<String, String>> rs = DbUtils.execute(sql);
		for (Map<String, String> maprs : rs) {
			parent_ids+=maprs.get("id")+",";
			redisutil.set("categoryid_"+maprs.get("id"), 1+"");
			
		}
		parent_ids = parent_ids.substring(0, parent_ids.length()-1);
		String sqlcatetwo = "select id,category_name from comd_category where parent_id in ("+parent_ids+")";
		logger.info(sqlcatetwo);
		List<Map<String, String>> rstwo = DbUtils.execute(sqlcatetwo);
		for (Map<String, String> maprstwo : rstwo) {
			redisutil.set("categoryid_"+maprstwo.get("id"), 2+"");
		}
		String sqlcatethr = "select category_id  from comd_category_meta where leaf_node=1 and root_id=category_id";
		List<Map<String, String>> rsthr = DbUtils.execute(sqlcatethr);
		for (Map<String, String> maprsthr : rsthr) {
			redisutil.set("categoryid_"+maprsthr.get("category_id"), 3+"");
		}
	}
	/***
	 * 
	 * */
	public static void categoryIdParentToReids(){
		RedisUtil  redisutil = new RedisUtil();

		String sql = "select id,parent_id from comd_category";
		List<Map<String, String>> rs = DbUtils.execute(sql);
		for (Map<String, String> maprs : rs) {
			redisutil.set("parentid_"+maprs.get("id"),maprs.get("parent_id"));	
		}
	
	}
	/***
	 * 分类ID=》key
	 * 分类名称=>value
	 * */
	public static void categoryNameToReids(){
		RedisUtil  redisutil = new RedisUtil();
		String sql = "select id,category_name from comd_category ";
		List<Map<String, String>> rs = DbUtils.execute(sql);
		for (Map<String, String> maprs : rs) {
			redisutil.set("categoryname_"+maprs.get("id"), maprs.get("category_name"));
		}
	
	}
	/***
	 * 品牌ID=》key
	 * 品牌名称=>value
	 * */
	public static void brandNameToReids(){
		RedisUtil  redisutil = new RedisUtil();
		String sql = "select id,brand_name from comd_brand  ";
		List<Map<String, String>> rs = DbUtils.execute(sql);
		for (Map<String, String> maprs : rs) {
			redisutil.set("brandname_"+maprs.get("id"), maprs.get("brand_name"));
		}
	
	}
	public static void enterpriseToRedis(){
		String sql ="select id,enterprise_name from uc_enterprise";
		RedisUtil  redisutil = new RedisUtil();
		List<Map<String, String>> rs = DbUtils.execute(sql);
		for (Map<String, String> maprs : rs) {
			String enterprise_name = maprs.get("enterprise_name");
			String id = maprs.get("id");
			redisutil.set("es_"+enterprise_name,id);
			int num_one = enterprise_name.indexOf("有限公司");
			int num_two = enterprise_name.indexOf("公司");
			int num_thr = enterprise_name.indexOf("有限责任公司");
			redisutil.set(enterprise_name,id);
			logger.info("key=es_"+enterprise_name+"--"+id);
			if(num_one >0){
				redisutil.set("es_"+enterprise_name.substring(0, num_one),id);
				logger.info("key=es_"+enterprise_name.substring(0, num_one)+"--"+id);
			}else if(num_two >0){
				redisutil.set("es_"+enterprise_name.substring(0, num_two), id);
				logger.info("key=es_"+enterprise_name.substring(0, num_two)+"--"+id);
			}else if(num_thr>0){
				redisutil.set("es_"+enterprise_name.substring(0, num_thr), id);
				logger.info("key=es_"+enterprise_name.substring(0, num_thr)+"--"+id);
			}
			
			
			
		}
	}
}

