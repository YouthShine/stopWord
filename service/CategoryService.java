package data.yunsom.com.service;

import java.util.List;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import data.yunsom.com.util.RedisUtil;

/**
 * 
 * 更新商品分类名称
 * */
public class CategoryService {
	private static RedisUtil redisutil = new RedisUtil();

	public void updateCategory(EventType eventType, List<Column> beforeColumns,
			List<Column> afterColumns) {
		int numSize = beforeColumns.size();
		String category_id = "";
		String name = "";
		if (eventType == EventType.INSERT) {
			for (Column data : afterColumns) {
				if (data.getName().equals("id")) {
					category_id = data.getValue();
				}
				if (data.getName().equals("category_name")) {
					name = data.getValue();
				}
			}
			String type = DataCategory.getCategory(category_id);
			String parent_id = DataCategory.getParentCategory(category_id);
			redisutil.set("es_" + name, category_id + "_" + type);
			redisutil.set("categoryname_" + category_id, name);
			redisutil.set("categoryid_" + category_id, type);
			redisutil.set("parentid_" + category_id, parent_id);

		}
		if (eventType == EventType.UPDATE) {
			for (int i = 0; i < numSize; i++) {
				if (beforeColumns.get(i).getName().equals("id")) {
					category_id = beforeColumns.get(i).getValue();
				}
				if (beforeColumns.get(i).getName().equals("category_name")
						&& !beforeColumns.get(i).getValue().trim()
								.equals(afterColumns.get(i).getValue().trim())) {
					name = afterColumns.get(i).getValue();

				}
			}
			if (!category_id.equals("") && !name.equals("")) {
				String type = DataCategory.getCategory(category_id);
				String parent_id = DataCategory.getParentCategory(category_id);
				redisutil.set("es_" + name, category_id + "_" + type);
				redisutil.set("categoryname_" + category_id, name);
				redisutil.set("categoryid_" + category_id, type);
				redisutil.set("parentid_" + category_id, parent_id);
			}
		} else if (eventType == EventType.DELETE) {
			for (Column data : afterColumns) {
				if (data.getName().equals("id")) {
					category_id = data.getValue();
				}
				if (data.getName().equals("category_name")) {
					name = data.getValue();
				}
			}

			redisutil.del("es_" + name);
			redisutil.del("categoryname_" + category_id);
			redisutil.del("categoryid_" + category_id);
			redisutil.del("parentid_" + category_id);
		}
	}
}
