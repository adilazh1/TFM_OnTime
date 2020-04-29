package com.fly.ontime.util;

import java.util.HashMap;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;

public class ConfigBundle
{
	private HashMap queries;
	private static ConfigBundle instance = new ConfigBundle();


	private ConfigBundle()
	{
		try {
			queries = new HashMap();
			loadFile(AppParams.XML_CONF, AppParams.TAG_CONF);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static ConfigBundle getInstance() {
		return instance;
	}

	/**
	 * 
	 * @param queryName
	 * @return
	 */
	public String getQuery(String queryName) 
	{
		return (String) queries.get(queryName);
	}

	/**
	 * 
	 * @param xml_file
	 * @param tag_xml
	 * @throws Exception
	 */
	private void loadFile(String xml_file,String tag_xml) throws Exception{

		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setValidating(false);
		Document doc = factory.newDocumentBuilder().parse(getClass().getClassLoader().getResourceAsStream(xml_file));
		NodeList nl = doc.getElementsByTagName(tag_xml);
		int nlLength = nl.getLength();
		for (int i = 0; i < nlLength; ++i) {
			Element queryTag = (Element) nl.item(i);
			Node n = queryTag.getChildNodes().item(0);
			String value = (n == null) ? "" : n.getNodeValue();
			queries.put(queryTag.getAttribute("name"), value);
		}

	}

}
