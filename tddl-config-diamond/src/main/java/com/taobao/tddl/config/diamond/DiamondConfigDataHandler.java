package com.taobao.tddl.config.diamond;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import com.taobao.diamond.manager.ManagerListener;
import com.taobao.diamond.manager.impl.DefaultDiamondManager;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.extension.Activate;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.common.utils.mbean.TddlMBean;
import com.taobao.tddl.common.utils.mbean.TddlMBeanServer;
import com.taobao.tddl.config.ConfigDataListener;
import com.taobao.tddl.config.impl.UnitConfigDataHandler;

/**
 * 持久配置中心diamond实现
 * 
 * @author shenxun
 * @author <a href="zylicfc@gmail.com">junyu</a>
 * @version 1.0
 * @since 1.6
 * @date 2011-1-11 11:22:29
 */
@Activate(order = 1)
public class DiamondConfigDataHandler extends UnitConfigDataHandler {

    private static final Logger logger  = LoggerFactory.getLogger(DiamondConfigDataHandler.class);
    public static final long    TIMEOUT = 10 * 1000;
    private String              mbeanId;
    private TddlMBean           mbean;
    private DefaultDiamondManager env;

    public void doInit() {
        mbean = new TddlMBean("Diamond Config Info " + System.currentTimeMillis());
        mbeanId = dataId + System.currentTimeMillis();

        // TODO 外部直接指定ip进行访问
        env = new DefaultDiamondManager(dataId, new ArrayList<ManagerListener>(0));

        if (initialData == null) {
            initialData = getData(TIMEOUT, FIRST_SERVER_STRATEGY);
        }
        addListener0(listeners, (Executor) config.get("executor"), initialData);
        TddlMBeanServer.registerMBeanWithId(mbean, mbeanId);
    }

    public String getNullableData(long timeout, String strategy) {
        String data = null;
        try {
            data = env.getConfigureInfomation(timeout);
        } catch (Exception e) {
            // 不抛异常，只记录一下
            logger.error(e);
        }

        if (data != null) {
            mbean.setAttribute(dataId, data);
        } else {
            mbean.setAttribute(dataId, "");
        }

        return data;
    }

    public String getData(long timeout, String strategy) {
        String data = null;
        try {
            data = env.getConfigureInfomation(timeout);
        } catch (Exception e) {
            throw new RuntimeException("get diamond data error!dataId:" + dataId, e);
        }

        if (data != null) {
            mbean.setAttribute(dataId, data);
        } else {
            mbean.setAttribute(dataId, "");
        }

        return data;
    }

    public void addListener(final ConfigDataListener configDataListener, final Executor executor) {
        if (configDataListener != null) {
            String data = getData(TIMEOUT, FIRST_SERVER_STRATEGY);
            addListener0(configDataListener, executor, data);
        }
    }

    public void addListeners(final List<ConfigDataListener> configDataListenerList, final Executor executor) {
        if (configDataListenerList != null) {
            String data = getData(TIMEOUT, FIRST_SERVER_STRATEGY);
            addListener0(configDataListenerList, executor, data);
        }
    }

    public void closeUnderManager() {
    	env.setManagerListeners(new ArrayList<ManagerListener>(0));
    }

    protected void doDestory() throws TddlException {
        closeUnderManager();
    }

    /**
     * 共用的addListener处理
     * 
     * @param configDataListenerList
     * @param executor
     * @param data
     */
    private void addListener0(final ConfigDataListener configDataListener, final Executor executor, String data) {
        env.addManagerListener(new ManagerListener() {

            @Override
            public Executor getExecutor() {
                return executor;
            }

            @Override
            public void receiveConfigInfo(String data) {
                configDataListener.onDataRecieved(dataId, data);
                if (data != null) {
                    mbean.setAttribute(dataId, data);
                } else {
                    mbean.setAttribute(dataId, "");
                }
            }
        });
    }

    /**
     * 共用的addListener处理
     * 
     * @param configDataListenerList
     * @param executor
     * @param data
     */
    private void addListener0(final List<ConfigDataListener> configDataListenerList, final Executor executor,
                              String data) {
        env.addManagerListener(new ManagerListener() {

            @Override
            public Executor getExecutor() {
                return executor;
            }

            @Override
            public void receiveConfigInfo(String data) {
                for (ConfigDataListener configDataListener : configDataListenerList) {
                    try {
                        configDataListener.onDataRecieved(dataId, data);
                    } catch (Exception e) {
                        logger.error("one of listener failed", e);
                        continue;
                    }
                }

                if (data != null) {
                    mbean.setAttribute(dataId, data);
                } else {
                    mbean.setAttribute(dataId, "");
                }
            }
        });
    }

    public String getDataId() {
        return dataId;
    }

    public void setDataId(String dataId) {
        this.dataId = dataId;
    }
}
