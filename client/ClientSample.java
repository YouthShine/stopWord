package data.yunsom.com.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.Message;

import data.yunsom.com.common.Config;
import data.yunsom.com.common.DbConfig;
import data.yunsom.com.kmeans.Program;
import data.yunsom.com.service.BrandService;
import data.yunsom.com.service.CategoryService;
import data.yunsom.com.service.CategoryThree;
import data.yunsom.com.service.CommodityAttrSearch;
import data.yunsom.com.service.CommodityPlaform;
import data.yunsom.com.service.CommodityPrivPriceService;
import data.yunsom.com.service.CommodityResult;
import data.yunsom.com.service.DeleteCommodity;
import data.yunsom.com.service.SyncFile;
import data.yunsom.com.service.SynchBrand;
import data.yunsom.com.service.SynchCategory;
import data.yunsom.com.service.SynchExcel;
import data.yunsom.com.service.SynchPinyinData;
import data.yunsom.com.service.SynchRedis;
import data.yunsom.com.service.SynchroData;
import data.yunsom.com.util.CommodityUtil;
import data.yunsom.com.util.DbUtils;
import data.yunsom.com.util.ElasticsearchUtil;
import data.yunsom.com.util.SuggestPinyinDataUtil;

/**
 * 数据同步 业务逻辑处理
 * 
 * @author yangxuchuan 2016/8/30
 */
public class ClientSample {
	//public static TransportClient client = ElasticsearchUtil
			//.getTransportClient();

	private static final Logger logger = LoggerFactory
			.getLogger(ClientSample.class);
	private static HashMap<String, Integer> tableMap = new HashMap<String, Integer>();
	
	public static void main(String args[]) throws InterruptedException,
			ExecutionException, IOException, SQLException {
		
		int day = 20161121;
		//SyncFile.fileToElastic(day);
		//SyncFile.dealTag();
		//CategoryThree.categorySerch();
		CommodityAttrSearch.attrSerch();
		
	}
		
		/*
		int param = 0;
		int commodity_id= 0;
		String path = "";
		if (args.length > 0) {
			param = Integer.parseInt(args[0]);
			commodity_id = Integer.parseInt(args[1]);
			path = args[2];
		}
		switch (param) {
		case 1:
			SynchRedis.mysqlToRedis();
			break;
		case 2:
			SynchPinyinData.pushCategoryData();
			SynchPinyinData.pushBrandData();
			SynchPinyinData.pushEnterpriseData();
			SynchroData.getData(commodity_id);
			break;
		case 0:
			constantlySync();
			break;
		case 4:
			SyncFile.fileToElastic(commodity_id);
			break;
		case 5:
			DeleteCommodity.delete();
			break;
		case 6:
			SynchBrand.insertBrand();
			break;
		case 3:
			SynchExcel.synchExcelPrint();
			break;
		case 7:
			SynchCategory.insertCategory();
			break;
		case 8:
			Program.kmeans(commodity_id, path);
		default:
			constantlySync();
			break;
		}
	
		
	}*/
/*
	private static void constantlySync() throws InterruptedException,
			IOException, ExecutionException, SQLException {
		CanalConnector connector = CanalConnectors.newSingleConnector(
				new InetSocketAddress(AddressUtils.getHostIp(), 11111), "example", "",
				"");
		int batchSize = 100;
		try {
			connector.connect();
			connector.subscribe(".*\\..*");
			connector.rollback();
			while (true) {
				logger.info("start sychro data");
				Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据

				long batchId = message.getId();
				int size = message.getEntries().size();
				if (batchId == -1 || size == 0) {
					try {
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				} else {
					printEntry(message.getEntries());
				}

				connector.ack(batchId); // 提交确认

			}

		} finally {
			connector.disconnect();
		}
	}
/*
	private static void printEntry(List<Entry> entrys)
			throws InterruptedException, IOException, ExecutionException,
			SQLException {

		for (Entry entry : entrys) {
			if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN
					|| entry.getEntryType() == EntryType.TRANSACTIONEND) {
				continue;
			}

			RowChange rowChage = null;
			try {
				rowChage = RowChange.parseFrom(entry.getStoreValue());
			} catch (Exception e) {

				logger.error("ERROR ## parser of eromanga-event has an error , data:"
						+ entry.toString() + ";message=" + e.getMessage());

			}
			tableMap = getTableMap();
			EventType eventType = rowChage.getEventType();
			String schema = entry.getHeader().getSchemaName();
			String tableName = entry.getHeader().getTableName();
			logger.info("schema==" + schema + "---tableName==" + tableName);
			boolean status = tableMap.get(tableName) == null ? false : true;
			if (schema.equals(DbConfig.SCHEMANAME) && status) {
				logger.info(String
						.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
								entry.getHeader().getLogfileName(), entry
										.getHeader().getLogfileOffset(), entry
										.getHeader().getSchemaName(), entry
										.getHeader().getTableName(), eventType));

				for (RowData rowData : rowChage.getRowDatasList()) {

					List<Column> beforeColumns = rowData.getBeforeColumnsList();
					List<Column> afterColumns = rowData.getAfterColumnsList();
					logger.info("before columns----");
					printColumn(beforeColumns);
					logger.info("after columns----");
					printColumn(afterColumns);
					switch (tableMap.get(tableName)) {
					case 4:
						if (eventType == EventType.UPDATE) {
							updateDocument(afterColumns);
						}
						break;

					 case 1:
					 BrandService brandService = new BrandService();
					 brandService.updateBrand(eventType, beforeColumns,
					 afterColumns);
					 break;
					 case 2:
					 CategoryService cateservice = new CategoryService();
					 cateservice.updateCategory(eventType, beforeColumns,
					 afterColumns);
					// case 7:
					// EnterpriseService entservice = new EnterpriseService();
					// entservice.updateEnterprise(eventType, beforeColumns,
					// afterColumns);
					// break;

					case 3:
						CommodityPlaform plaform = new CommodityPlaform();
						plaform.updatePlaform(eventType, beforeColumns,
								afterColumns);
						break;

					case 6:
						CommodityPrivPriceService priprice = new CommodityPrivPriceService();
						priprice.updatePrivPrice(eventType, beforeColumns,
								afterColumns);
						break;
					default:
						break;
					}

				}

			}
		}
	}
*/
	/**
	 * 添加文档
	 * 
	 * @param columns
	 * @throws SQLException
	 * @throws Exception
	 */
	/*
	public static void addDocument(List<Column> columns)
			throws InterruptedException, ExecutionException, IOException,
			SQLException {

		BulkRequestBuilder bulkRequest = client.prepareBulk();
		CommodityResult result = new CommodityResult();
		String commodity_id = CommodityUtil.getCommodityId(columns, "id");
		String name = CommodityUtil.getCommodityId(columns, "commodity_name");
		String pinyinJson = SuggestPinyinDataUtil.getSuggestPinyin(name,
				Config.TYPE_WEIGHT_COMMONDITY);
		logger.info("addDocument pinyinJson--" + pinyinJson);
		bulkRequest.add(client.prepareIndex(Config.SUGGEST, Config.TYPE,
				"commodity_" + commodity_id).setSource(pinyinJson));
		HashMap<String, String> map = result.combinData(columns);
		for (java.util.Map.Entry<String, String> entry : map.entrySet()) {
			bulkRequest.add(client.prepareIndex(Config.INDEXNAME, Config.TYPE,
					entry.getKey()).setSource(entry.getValue()));
			logger.info("addDocument datajosn--" + entry.getValue());
		}

		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			logger.error(bulkResponse.buildFailureMessage());
		}

	}
*/
	/**
	 * 更新文档
	 * 
	 * @param type
	 * @param indexName
	 * @param columns
	 * @throws SQLException
	 * @throws Exception
	 */
	/*
	public static void updateDocument(List<Column> afterColumns)
			throws IOException, InterruptedException, ExecutionException,
			SQLException {
		//BulkRequestBuilder bulkRequest = client.prepareBulk();
		String deleted_at = CommodityUtil.getCommodityId(afterColumns,
				"deleted_at");
		int status = Integer.parseInt(CommodityUtil.getCommodityId(
				afterColumns, "status"));
		int is_display = Integer.parseInt(CommodityUtil.getCommodityId(
				afterColumns, "is_display"));
		String commodity_id = CommodityUtil.getCommodityId(afterColumns, "id");
		//String name = CommodityUtil.getCommodityId(afterColumns,
			//	"commodity_name");
		if(deleted_at.length()>8 || status==2 || is_display==2){
			logger.info("delete commodity_id");
			deleteDocument(commodity_id);
		}else{
			
			CommodityResult result = new CommodityResult();
			HashMap<String, String> map = result.combinData(afterColumns);
			for (java.util.Map.Entry<String, String> entry : map.entrySet()) {
				bulkRequest.add(client.prepareUpdate(Config.INDEXNAME,
						Config.TYPE, entry.getKey()).setDoc(entry.getValue()));
				logger.info("update datajosn--" + entry.getValue());
			}
			BulkResponse bulkResponse = bulkRequest.execute().actionGet();
			if (bulkResponse.hasFailures()) {
				logger.error(bulkResponse.buildFailureMessage());
				for (java.util.Map.Entry<String, String> entry : map.entrySet()) {
					bulkRequest.add(client.prepareUpdate(Config.INDEXNAME,
							Config.TYPE, entry.getKey()).setDoc(entry.getValue()));
					logger.info("insert datajosn--" + entry.getValue());
				}
				BulkResponse bulkResponseinsert = bulkRequest.execute().actionGet();
				if(bulkResponseinsert.hasFailures()){
					logger.error(bulkResponse.buildFailureMessage());
				}
			}

		}
	}
*/
	/**
	 * 删除文档
	 * 
	 * @param type
	 * @param indexName
	 * @param columns
	 * @throws Exception
	 */
	
	private static void deleteDocument(String commodity_id) {

	
		String sql = "select  platform_id from  comd_commodity_platform_ref where commodity_id="+commodity_id;
		List<Map<String, String>> rs = DbUtils.execute(sql);
		for (Map<String, String> map : rs) {
			String platform_id = map.get("platform_id");
			logger.info(commodity_id+"---"+platform_id);
			CommodityPlaform.deleteData(commodity_id, platform_id); 
		}

	}

	public static void printColumn(List<Column> columns) {
		for (Column column : columns) {
			System.out.println(column.getName() + "---" + column.getValue());
		}
	}

	public static HashMap<String, Integer> getTableMap() {
		HashMap<String, Integer> tableMap = new HashMap<String, Integer>();
		tableMap.put("comd_brand", 1);
		tableMap.put("comd_category", 2);
		tableMap.put("comd_commodity_platform_ref", 3);
		tableMap.put("comd_commodity", 4);
		tableMap.put("comd_commodity_priv_price_ref", 6);
		tableMap.put("uc_enterprise", 7);
		return tableMap;
	}

}
