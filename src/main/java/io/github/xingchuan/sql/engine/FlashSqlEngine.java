package io.github.xingchuan.sql.engine;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.io.resource.ResourceUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import io.github.xingchuan.sql.exception.FlashSqlEngineException;
import io.github.xingchuan.sql.provider.SqlParseProvider;
import io.github.xingchuan.sql.xml.XmlDocumentParser;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static io.github.xingchuan.sql.provider.impl.DefaultMybatisSqlParseProvider.MYBATIS_SQL_TYPE;


/**
 * 模板的引擎类，用于根据模板生成sql内容
 *
 * @author xingchuan.qxc
 * @since 1.0
 */
public class FlashSqlEngine {

    private final Logger logger = LoggerFactory.getLogger(FlashSqlEngine.class);

    /**
     * 从配置文件读出来的sqlId映射，key -> sqlId value -> sql模板内容
     */
    private final Map<String, String> sqlIdMap = new HashMap<>();


    /**
     * sql转换内容的提供类
     */
    private final Map<String, SqlParseProvider> sqlParseProviderMap = new HashMap<>();

    /**
     * 注册一个sql转换类型
     *
     * @param typeCode sql转换器类型code
     * @param provider sql转换器对象
     */
    public void registerSqlParseProvider(String typeCode, SqlParseProvider provider) {
        this.sqlParseProviderMap.put(typeCode, provider);
        logger.info("sqlParseProvider {} registered ok.", typeCode);
    }

    /**
     * 根据configFilePath，初始化内容
     *
     * @param configFilePath 待加载的资源位置，必须在mapper目录下，文件格式为xml，支持通配符*，例如 mapper/aa/*.xml,mapper/**\/*.xml
     * @throws IOException IO异常
     */
    public void loadConfig(String configFilePath) throws IOException {
        Date startDate = DateUtil.date();
        List<Path> xmlFiles = getFilesInDirectory(configFilePath);
        for (Path path : xmlFiles) {
            try (InputStream inputStream = loadConfigSourceStream(path.toAbsolutePath().toString())) {
                Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(inputStream);
                // 获取namespace
                Element mapperElement = (Element) document.getElementsByTagName("mapper").item(0);
                String namespace = mapperElement.getAttribute("namespace");
                sqlIdMap.putAll(XmlDocumentParser.fetchXmlDocumentSql(document, "select", namespace, sqlIdMap));
                sqlIdMap.putAll(XmlDocumentParser.fetchXmlDocumentSql(document, "update", namespace, sqlIdMap));
                sqlIdMap.putAll(XmlDocumentParser.fetchXmlDocumentSql(document, "insert", namespace, sqlIdMap));
                sqlIdMap.putAll(XmlDocumentParser.fetchXmlDocumentSql(document, "delete", namespace, sqlIdMap));
            } catch (ParserConfigurationException | SAXException e) {
                throw new FlashSqlEngineException(e.getMessage());
            }
        }
        Date endDate = DateUtil.date();
        logger.info("{} sql template loading completed, the loading lasted {} seconds, a total of {} files, {} templates.", DateUtil.format(endDate, DatePattern.NORM_DATETIME_PATTERN), DateUtil.between(startDate, endDate, DateUnit.SECOND), xmlFiles.size(), sqlIdMap.size());
    }

    /**
     * 根据configFilePath，初始化内容
     *
     * @param inputStream 待加载的资源
     * @throws IOException IO异常
     */
    public void loadConfig(InputStream inputStream) throws IOException {
        Date startDate = DateUtil.date();
        try {
            Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(inputStream);
            // 获取namespace
            Element mapperElement = (Element) document.getElementsByTagName("mapper").item(0);
            String namespace = mapperElement.getAttribute("namespace");
            sqlIdMap.putAll(XmlDocumentParser.fetchXmlDocumentSql(document, "select", namespace, sqlIdMap));
            sqlIdMap.putAll(XmlDocumentParser.fetchXmlDocumentSql(document, "update", namespace, sqlIdMap));
            sqlIdMap.putAll(XmlDocumentParser.fetchXmlDocumentSql(document, "insert", namespace, sqlIdMap));
            sqlIdMap.putAll(XmlDocumentParser.fetchXmlDocumentSql(document, "delete", namespace, sqlIdMap));
        } catch (SAXException e) {
            throw new RuntimeException(e);
        } catch (ParserConfigurationException e) {
            throw new RuntimeException(e);
        }
        Date endDate = DateUtil.date();
        logger.info("{} sql template loading completed, the loading lasted {} seconds, {} templates in total.", DateUtil.format(endDate, DatePattern.NORM_DATETIME_PATTERN), DateUtil.between(startDate, endDate, DateUnit.SECOND), sqlIdMap.size());
    }


    /**
     * 新增一个sql模板
     *
     * @param sqlId    sqlId
     * @param template 模板
     * @since 1.0.1
     */
    public void saveSqlTemplate(String sqlId, String template) {
        if (sqlIdMap.containsKey(sqlId)) {
            logger.error("sqlId {} already exists. ", sqlId);
            throw new FlashSqlEngineException("sqlId " + sqlId + " already exists. ");
        }
        this.sqlIdMap.put(sqlId, template);
        logger.info("sqlId {} added. ", sqlId);
    }

    /**
     * 获取一个sql模板
     *
     * @param sqlId sqlId
     * @return 对应的模板content
     * @since 1.0.1
     */
    public String fetchSqlTemplate(String sqlId) {
        return this.sqlIdMap.get(sqlId);
    }


    /**
     * 转换sqlId 的内容成为可执行的sql
     *
     * @param sqlId  配置文件中的sqlId
     * @param params 构建的参数Json对象
     * @return 渲染完成的sql
     */
    public String parseSqlWithSqlId(String sqlId, JSONObject params) {
        return parseSqlWithSqlId(sqlId, params, MYBATIS_SQL_TYPE);
    }

    /**
     * 转换sqlId 的内容成为可执行的sql
     *
     * @param sqlId        配置文件中的sqlId
     * @param params       构建的参数Json对象
     * @param providerType 转换器类型code
     * @return 渲染完成的sql
     */
    public String parseSqlWithSqlId(String sqlId, JSONObject params, String providerType) {
        if (StrUtil.isBlank(sqlId)) {
            return StrUtil.EMPTY;
        }
        String sqlTemplateContent = fetchSqlTemplate(sqlId);
        if (StrUtil.isBlank(sqlTemplateContent)) {
            return StrUtil.EMPTY;
        }
        return parseSql(sqlTemplateContent, params, providerType);
    }

    /**
     * 转换成为可以执行的sql
     *
     * @param template 模板内容
     * @param params   构建的参数JSON对象
     * @return 渲染完成的sql
     */
    public String parseSql(String template, JSONObject params, String providerType) {
        SqlParseProvider sqlParseProvider = sqlParseProviderMap.get(providerType);
        if (sqlParseProvider == null) {
            throw new IllegalArgumentException("provider " + providerType + " not found");
        }
        return sqlParseProvider.parseSql(template, params);
    }


    /**
     * 将path转换成对应的字节流
     *
     * @param configFilePath 配置文件的位置
     * @return 对应的字节输入流
     */
    private InputStream loadConfigSourceStream(String configFilePath) {
        File configFile = FileUtil.file(configFilePath);
        boolean exist = FileUtil.exist(configFile);
        if (!exist) {
            // 不是一个标准文件，从类路径下获取一次
            return ResourceUtil.getStream(configFilePath);
        }
        return IoUtil.toStream(FileUtil.readBytes(configFile));
    }

    /**
     * 获取加载文件
     *
     * @param mapperLocations 加载文件路径
     * @return 待加载文件路径
     */
    private List<Path> getFilesInDirectory(String mapperLocations) {
        // 默认场合加载mapper/iotdb/路径下的xml文件
        String directoryPath = "mapper/iotdb/";
        String filePattern = "*.xml";
        if (StrUtil.isNotEmpty(mapperLocations)) {
            int index1 = StrUtil.indexOf(mapperLocations, "mapper", 0, true);
            int index2 = StrUtil.indexOf(mapperLocations, '*');
            directoryPath = StrUtil.sub(mapperLocations, index1, index2);
        }
        List<Path> files = new ArrayList<>();
        ClassLoader classLoader = FileUtil.class.getClassLoader();
        URL resource = classLoader.getResource(directoryPath);
        if (resource != null) {
            try {
                Path path = Paths.get(resource.toURI());
                Files.walk(path)
                        .filter(Files::isRegularFile)
                        .filter(p -> FilenameUtils.wildcardMatch(p.getFileName().toString(), filePattern))
                        .forEach(files::add);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return files;
    }
}
