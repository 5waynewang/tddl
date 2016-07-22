package com.taobao.tddl.config.diamond;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.diamond.client.DiamondClientPool;
import com.taobao.tddl.common.utils.extension.Activate;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.config.impl.holder.AbstractConfigDataHolder;

@Activate(order = 1)
public class DiamondConfigHolder extends AbstractConfigDataHolder {

    private static final Logger log = LoggerFactory.getLogger(DiamondConfigHolder.class);

    public void doInit() {

    }

    protected Map<String, String> queryAndHold(List<String> dataIds, String unitName) {
        // TODO 外部直接指定ip进行访问
		Map<String, String> result = new HashMap<String, String>();
		for (String dataId : dataIds) {
			result.put(dataId, DiamondClientPool.getConfigure(dataId));
		}
		return result;
    }

    @Override
    public Map<String, String> getData(List<String> dataIds) {
        Map<String, String> result = new HashMap<String, String>();
        for (String dataId : dataIds) {
            result.put(dataId, getData(dataId));
        }
        return result;
    }

    @Override
    public String getData(String dataId) {
        return configHouse.containsKey(dataId) ? configHouse.get(dataId) : getDataFromSonHolder(dataId);
    }

}
