package data.yunsom.com.service;

import java.util.List;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import data.yunsom.com.util.RedisUtil;

/**
 * 更新商品品牌名称
 * */
public class BrandService {

	private static RedisUtil redisutil = new RedisUtil();

	public void updateBrand(EventType eventType, List<Column> beforeColumns,
			List<Column> afterColumns) {
		int numSize = afterColumns.size();
		String brand_id = "";
		String name = "";
		if (eventType == EventType.INSERT) {
			for (Column data : afterColumns) {
				if (data.getName().equals("id")) {
					brand_id = data.getValue();
				}
				if (data.getName().equals("brand_name")) {
					name = data.getValue();
				}
			}
			redisutil.set("brandname_" + brand_id, name);

		} else if (eventType == EventType.UPDATE) {
			for (int i = 0; i < numSize; i++) {
				if (beforeColumns.get(i).getName().equals("id")) {
					brand_id = beforeColumns.get(i).getValue();
				}
				if (beforeColumns.get(i).getName().equals("brand_name")
						&& !beforeColumns.get(i).getValue().trim()
								.equals(afterColumns.get(i).getValue().trim())) {
					name = afterColumns.get(i).getValue();

				}
			}
			if (!brand_id.equals("") && !name.equals("")) {
				redisutil.set("brandname_" + brand_id, name);
			}
		} else if (eventType == EventType.DELETE) {
			for (Column data : afterColumns) {
				if (data.getName().equals("id")) {
					brand_id = data.getValue();
				}
				if (data.getName().equals("brand_name")) {
					name = data.getValue();
				}
			}
			redisutil.del("brandname_" + brand_id);
		}

	}

}
