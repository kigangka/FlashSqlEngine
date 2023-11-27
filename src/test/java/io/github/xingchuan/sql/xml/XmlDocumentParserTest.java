package io.github.xingchuan.sql.xml;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

/**
 * @author xingchuan.qxc
 * @since 1.0
 */
public class XmlDocumentParserTest {

    private Logger logger = LoggerFactory.getLogger(XmlDocumentParserTest.class);

    @Test
    public void test_xml_config_file_read() throws ParserConfigurationException, IOException, SAXException {
        String configFilePath = "test-sql-mapper.xml";
        ByteArrayInputStream inputStream = IoUtil.toStream(FileUtil.readBytes(configFilePath));
        Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(inputStream);
        // 获取namespace
        Element mapperElement = (Element) document.getElementsByTagName("mapper").item(0);
        String namespace = mapperElement.getAttribute("namespace");
        Map<String, String> sqlIdMap = XmlDocumentParser.fetchXmlDocumentSql(document, "select", namespace, new HashMap<>());
        logger.info("{}", sqlIdMap);
        String testQuerySqlTemplateContent = sqlIdMap.get("testQuery");
        assertThat(testQuerySqlTemplateContent).isNotEmpty();
    }
}