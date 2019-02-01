package com.yd.spark.util;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
  * Created by suibianda LGD  on 2019/2/1 14:38
  * Modified by: 
  * Version: 0.0.1
  * Usage: 加载properties文件工具类
  *
  */
public class PropertiesLoader {
	private static final Logger logger = LoggerFactory.getLogger(PropertiesLoader.class);
	private Properties props;

	/**
	 * fileName指定要加载的properties文件
	 */
	public static PropertiesLoader getInstance() {
		return new PropertiesLoader("/config-prd.properties", false);
	}

	public PropertiesLoader(String fileName, boolean isAbsolutePath) {
		InputStream in = null;
		FileInputStream fis = null;
		try {
			this.props = new Properties();
			if (isAbsolutePath) {
				// 绝对路径
				fis = new FileInputStream(fileName);
				in = new BufferedInputStream(fis);
				this.props.load(in);
			} else {
				// 相对路径
				in = getClass().getResourceAsStream(fileName);
				this.props.load(in);
			}
		} catch (Exception e) {
			logger.error("Loading properties file failed! fileName={}", fileName, e);
		} finally {
			// 安全关闭文件流
			IOUtils.closeQuietly(fis);
			IOUtils.closeQuietly(in);
		}
	}

	public String getProperty(String key) {
		return this.props.getProperty(key);
	}

	public int getProperty(String key, int defaultValue) {
		if (StringUtils.isEmpty(this.props.getProperty(key))) {
			return defaultValue;
		}
		return Integer.parseInt(this.props.getProperty(key));
	}

	public double getProperty(String key, double defaultValue) {
		if (StringUtils.isEmpty(this.props.getProperty(key))) {
			return defaultValue;
		}
		return Double.parseDouble(this.props.getProperty(key));
	}

	public long getProperty(String key, long defaultValue) {
		if (StringUtils.isEmpty(this.props.getProperty(key))) {
			return defaultValue;
		}
		return Long.parseLong(this.props.getProperty(key));
	}

	public boolean getProperty(String key, boolean defaultValue) {
		if (StringUtils.isEmpty(this.props.getProperty(key))) {
			return defaultValue;
		}
		return Boolean.getBoolean(this.props.getProperty(key));
	}

}
