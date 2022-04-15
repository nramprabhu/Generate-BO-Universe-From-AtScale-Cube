import java.util.LinkedList;
import java.util.List;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.io.File;
import java.io.IOException;

import com.crystaldecisions.sdk.exception.SDKException;
import com.crystaldecisions.sdk.framework.CrystalEnterprise;
import com.crystaldecisions.sdk.framework.IEnterpriseSession;
import com.sap.sl.sdk.authoring.datafoundation.DataFoundationFactory;
import com.sap.sl.sdk.authoring.datafoundation.DataFoundationView;
import com.sap.sl.sdk.authoring.datafoundation.DatabaseTable;
import com.sap.sl.sdk.authoring.datafoundation.MonoSourceDataFoundation;
import com.sap.sl.sdk.authoring.local.LocalResourceService;
import com.sap.sl.sdk.framework.SlContext;
import com.sap.sl.sdk.framework.cms.CmsSessionService;
import com.sap.sl.sdk.authoring.businesslayer.AccessLevel;
import com.sap.sl.sdk.authoring.businesslayer.BusinessLayerFactory;
import com.sap.sl.sdk.authoring.businesslayer.BusinessLayerView;
import com.sap.sl.sdk.authoring.businesslayer.DataType;
import com.sap.sl.sdk.authoring.businesslayer.Dimension;
import com.sap.sl.sdk.authoring.businesslayer.Folder;
import com.sap.sl.sdk.authoring.businesslayer.ItemState;
import com.sap.sl.sdk.authoring.businesslayer.Measure;
import com.sap.sl.sdk.authoring.businesslayer.ProjectionFunction;
import com.sap.sl.sdk.authoring.businesslayer.RelationalBinding;
import com.sap.sl.sdk.authoring.businesslayer.RelationalBusinessLayer;
import com.sap.sl.sdk.authoring.cms.CmsResourceService;
import com.sap.sl.sdk.authoring.datafoundation.TableState;
import com.sap.sl.sdk.authoring.datafoundation.TableView;
import com.sap.sl.sdk.authoring.connection.ConnectionFactory;
import com.sap.sl.sdk.authoring.connection.RelationalConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenerateUniverse {
    private static final Logger logger = LoggerFactory.getLogger(GenerateUniverse.class);
    public static void main(String[] args)  throws SDKException, ParserConfigurationException, SAXException, IOException {

        /** BO Enterprise CMS */
        final String CMS_NAME = args[0];

        /** BO Enterprise User Name */
        final String CMS_USER_NAME = args[1];

        /** BO Enterprise Password */
        final String CMS_PASSWORD = args[2];

        /** CMS AUTH MODE */
        final String CMS_AUTH_MODE = "secEnterprise";

        /** Local folder used to save all resources locally */
        final String LOCAL_FOLDER = "f:\\sl_sdk_folder\\";

        /** Qualifier Name - Project Name */
        final String CNX_QUALIFIER = "Hive";

        /** Hive JDBC Driver Name */
        final String HIVE_DRIVER_NAME = "Simba JDBC Drivers";

        /** Hive JDBC Driver Version */
        final String HIVE_DRIVER_VERSION = "Apache Hadoop Hive 1.0 HiveServer2";

        /** Hive JDBC Driver AuthMech Property */
        final String JDBC_EXTRA_PROPERTIES = "AuthMech=3";

        /** AtScale Server user name */
        final String ATSCALE_USER_NAME = "admin";

        /** AtScale Server password */
        final String ATSCALE_PASSWORD = "admin";


        // ** BO Login Session Creation

        // Creating a Session to BO Enterprise
        IEnterpriseSession enterpriseSession = null;
        SlContext context = SlContext.create();
        enterpriseSession = CrystalEnterprise.getSessionMgr().logon(CMS_USER_NAME, CMS_PASSWORD, CMS_NAME,
                CMS_AUTH_MODE);
        context.getService(CmsSessionService.class).setSession(enterpriseSession);



        for(int tdsCount = 3; tdsCount < args.length; tdsCount++) {

            String TDS_FILE_NAME = args[tdsCount];

            // Read the .tds file generated from AtScale Engine to gather Universe
            // properties

            File fXmlFile = new File(TDS_FILE_NAME);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(fXmlFile);
            doc.getDocumentElement().normalize();

            // Get Cube name & Project Name from tds file
            Element cubeNameElement = (Element) doc.getElementsByTagName("relation").item(0);
            logger.info("Reading Cube name from tds file - " + cubeNameElement.getAttribute("name").replaceAll(" ", "_"));
            logger.info("Reading Owner name from tds file - "
                    + cubeNameElement.getAttribute("table").replaceAll("\\]", "").replaceAll("\\[", "").split("\\.")[0]);

            // Get AtScale Server URL & PORT from tds file
            Element connectionElement = (Element) doc.getElementsByTagName("connection").item(0);
            String ServerName = connectionElement.getChildNodes().item(1).getChildNodes().item(1).getChildNodes().item(1).getAttributes().getNamedItem("server").getNodeValue();
            String ServerPort =connectionElement.getChildNodes().item(1).getChildNodes().item(1).getChildNodes().item(1).getAttributes().getNamedItem("port").getNodeValue();
            logger.info("Reading Server URL : Port from tds file - " + ServerName + ":"
                    + ServerPort );

            /** Cube Name */
            final String CUBE_NAME = cubeNameElement.getAttribute("name").replaceAll(" ", "_");

            /** Connection Name */
            final String CONNECTION_NAME = CUBE_NAME + "_AtScale_Created_JDBC";

            /** Data Foundation Name */
            final String DF_NAME = CUBE_NAME + "_DF";

            /** Business Layer Name */
            final String BL_NAME = CUBE_NAME + "_universe";

            /** Owner Name - Cube Name */
            final String CNX_OWNER = cubeNameElement.getAttribute("table").replaceAll("\\]", "").replaceAll("\\[", "")
                    .split("\\.")[0].toLowerCase();

            /** Table Name */
            final String TABLE_NAME = cubeNameElement.getAttribute("name").toLowerCase();
            logger.info("Table Name " + TABLE_NAME );

            /** OWNER_NAME.TABLE.NAME construction */
            final String OWNER_TABLE_NAME = "`" + CNX_OWNER.toLowerCase() + "`" + "." + "`" + TABLE_NAME.toLowerCase() + "`";
            logger.info("Fully Qualified Table Name " + OWNER_TABLE_NAME );

            /** AtScale Server URL:PORT */
            final String ATSCALE_URL_PORT = ServerName + ":" + ServerPort;

            // Get Folder List , Drill-Path List and Columns List from tds file
            NodeList folderList = doc.getElementsByTagName("folder");
            NodeList drillPathList = doc.getElementsByTagName("drill-path");
            NodeList columnList = doc.getElementsByTagName("column");


            // ** Connection Creation

            // Create a New Connection
            logger.info("Creating a new connection " + CONNECTION_NAME + ".cnx....");
            ConnectionFactory connectionFactory = context.getService(ConnectionFactory.class);
            RelationalConnection connection = connectionFactory.createRelationalConnection(CONNECTION_NAME,
                    HIVE_DRIVER_VERSION, HIVE_DRIVER_NAME);
            connection.getParameter("DATASOURCE").setValue(ATSCALE_URL_PORT);
            connection.getParameter("USER_NAME").setValue(ATSCALE_USER_NAME);
            connection.getParameter("PASSWORD").setValue(ATSCALE_PASSWORD);
            connection.getParameter("CONN_PROPERTIES").setValue(JDBC_EXTRA_PROPERTIES);

            // Saves the connection locally
            logger.info("Saving the Connection Locally in " + LOCAL_FOLDER + "....");
            LocalResourceService localResourceService = context.getService(LocalResourceService.class);
            localResourceService.save(connection, LOCAL_FOLDER + CONNECTION_NAME + ".cnx", true);

            // Publishes the connection to a CMS and retrieves a shortcut
            logger.info("Publishing the Connection to BOBJ Repository " + CMS_NAME + " as " + CONNECTION_NAME + ".cns....");
            CmsResourceService cmsResourceService = context.getService(CmsResourceService.class);
            String cnxCmsPath = cmsResourceService.publish(LOCAL_FOLDER + CONNECTION_NAME + ".cnx",
                    CmsResourceService.CONNECTIONS_ROOT, true);
            String shortcutPath = cmsResourceService.createShortcut(cnxCmsPath, LOCAL_FOLDER);

            // ** Single-source Data Foundation creation

            logger.info("Creating a new Data Foundation " + DF_NAME + " .....");
            DataFoundationFactory dataFoundationFactory = context.getService(DataFoundationFactory.class);
            MonoSourceDataFoundation dataFoundation = dataFoundationFactory.createMonoSourceDataFoundation(DF_NAME,
                    shortcutPath);

            // Adds tables to the data foundation
            logger.info("Adding cube to the Data Foundation .... ");
            DatabaseTable onlyTable = dataFoundationFactory.createDatabaseTable(CNX_QUALIFIER, CNX_OWNER, TABLE_NAME,
                    dataFoundation);

            // Adds a data foundation view and table views
            logger.info("Adding data foundation view \"AtScale foundation view\" and table views for " + TABLE_NAME
                    + " table ....");
            DataFoundationView dataFoundationView = dataFoundationFactory
                    .createDataFoundationView("AtScale foundation view", dataFoundation);
            dataFoundationView.setDescription("This is AtScale foundation view");
            TableView tableView = dataFoundationFactory.createTableView(onlyTable, dataFoundationView);

            // Sets table views properties
            logger.info("Setting table views properties on \"AtScale foundation view\"");
            logger.info("Setting TableState property \"EXPANDED\" to " + TABLE_NAME + " table view");
            logger.info("Setting TableState in Position \"(x=500,y=200)\" to " + TABLE_NAME + " in table view");
            logger.info("Setting TableState Width \"300px\" to " + TABLE_NAME + " in table view");
            tableView.setTableState(TableState.EXPANDED);
            tableView.setX(500);
            tableView.setY(200);
            tableView.setWidth(300);

            // Saves the data foundation
            logger.info("Saving data foundation locally into " + LOCAL_FOLDER + DF_NAME + ".dfx");
            localResourceService.save(dataFoundation, LOCAL_FOLDER + DF_NAME + ".dfx", true);

            //
            // ** Business Layer creation
            //

            BusinessLayerFactory businessLayerFactory = context.getService(BusinessLayerFactory.class);

            // Creates the business layer
            logger.info("Creating Business Layer " + BL_NAME);
            RelationalBusinessLayer businessLayer = businessLayerFactory.createRelationalBusinessLayer(BL_NAME,
                    LOCAL_FOLDER + DF_NAME + ".dfx");

            // Creates a business layer view in the business layer
            logger.info("Creating Business Layer View");
            BusinessLayerView businessLayerView = businessLayerFactory.createBusinessLayerView(TABLE_NAME + " view",
                    businessLayer);

            // Creates root folder that contains the business layer
            Folder rootFolder = addFolder(TABLE_NAME, "Root Folder for the Cube", businessLayer.getRootFolder(),
                    businessLayerFactory, businessLayerView);

            // Creates root folder for all Measures and Measure Folders , Create
            // Measure folder only when there is a TDS folder marked as Measure .
            Folder MeasuresFolder = rootFolder;
            int IsMesureFolderRequired = 0;
            for (int temp = 0; temp < folderList.getLength(); temp++) {
                Element eElement = (Element) folderList.item(temp);
                if (folderList.item(temp).getNodeType() == Node.ELEMENT_NODE) {
                    if (eElement.getAttribute("role").equals("measures")) {
                        IsMesureFolderRequired = 1;
                        break;
                    }

                }
            }
            if (IsMesureFolderRequired == 1) {
                MeasuresFolder = addFolder("Measures", "Measures Folder Under the Root", rootFolder, businessLayerFactory,
                        businessLayerView);
            }

            // Counters for Drill-path & Column
            List<String> drillPathCounter = new LinkedList<String>();
            List<String> columnCounter = new LinkedList<String>();

            // Loop into TDS file using folder list , drill path list and column list
            for (int temp = 0; temp < folderList.getLength(); temp++) {
                Node nNode = folderList.item(temp);
                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element eElement = (Element) nNode;
                    String folderName = eElement.getAttribute("name");
                    String folderRole = eElement.getAttribute("role");
                    Folder parentFolder = rootFolder;
                    if (folderRole.equals("measures")) {
                        parentFolder = MeasuresFolder;
                    }
                    Folder folder = addFolder(folderName, folderName, parentFolder, businessLayerFactory,
                            businessLayerView);
                    NodeList folderItemList = eElement.getElementsByTagName("folder-item");
                    for (int i = 0; i < folderItemList.getLength(); i++) {
                        Node folderItemNode = folderItemList.item(i);
                        if (folderItemNode.getNodeType() == Node.ELEMENT_NODE) {
                            Element folderItemElement = (Element) folderItemNode;
                            String folderItemName = folderItemElement.getAttribute("name");
                            String folderItemType = folderItemElement.getAttribute("type");
                            parentFolder = folder;
                            if (folderItemType.equals("drillpath")) {
                                for (int j = 0; j < drillPathList.getLength(); j++) {
                                    Node drillPathNode = drillPathList.item(j);
                                    if (drillPathNode.getNodeType() == Node.ELEMENT_NODE) {
                                        Element drillPathElement = (Element) drillPathNode;
                                        if (drillPathElement.getAttribute("name").equals(folderItemName)) {
                                            drillPathCounter.add(folderItemName);
                                            Folder folder1 = addFolder(drillPathElement.getAttribute("name"),
                                                    drillPathElement.getAttribute("name"), parentFolder,
                                                    businessLayerFactory, businessLayerView);
                                            NodeList fieldNodeList = drillPathElement.getElementsByTagName("field");
                                            for (int k = 0; k < fieldNodeList.getLength(); k++) {
                                                Element fieldElement = (Element) fieldNodeList.item(k);
                                                String fieldName = fieldElement.getTextContent().trim();
                                                for (int l = 0; l < columnList.getLength(); l++) {
                                                    Element columnElement = (Element) columnList.item(l);
                                                    parentFolder = folder1;
                                                    if (columnElement.getAttribute("name").equals(fieldName)) {
                                                        columnCounter.add(fieldName);
                                                        processField(columnElement, OWNER_TABLE_NAME, parentFolder,
                                                                businessLayerFactory, businessLayerView);

                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            } else {
                                if (folderItemType.equals("field")) {
                                    for (int l = 0; l < columnList.getLength(); l++) {
                                        Element columnElement = (Element) columnList.item(l);
                                        if (columnElement.getAttribute("name").equals(folderItemName)) {
                                            columnCounter.add(folderItemName);
                                            processField(columnElement, OWNER_TABLE_NAME, parentFolder,
                                                    businessLayerFactory, businessLayerView);
                                        }

                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Checking for Orphan Hierarchies & Field Elements (columns) which are
            // not there in any folders but directly under the root folder
            if (drillPathList.getLength() > drillPathCounter.size()) {
                for (int j = 0; j < drillPathList.getLength(); j++) {
                    Node drillPathNode = drillPathList.item(j);
                    if (drillPathNode.getNodeType() == Node.ELEMENT_NODE) {
                        Element drillPathElement = (Element) drillPathNode;
                        int needToHandleOrphanDrillPath = 1;
                        for (int k = 0; k < drillPathCounter.size(); k++) {
                            if (drillPathElement.getAttribute("name").equals(drillPathCounter.get(k))) {
                                needToHandleOrphanDrillPath = 0;
                                break;
                            }
                        }

                        if (needToHandleOrphanDrillPath == 1) { // This drill path was not handled
                            // initially , lets handle now and add
                            // it under root folder

                            drillPathCounter.add(drillPathElement.getAttribute("name"));
                            Folder folder1 = addFolder(drillPathElement.getAttribute("name"),
                                    drillPathElement.getAttribute("name"), rootFolder, businessLayerFactory,
                                    businessLayerView);
                            NodeList fieldNodeList = drillPathElement.getElementsByTagName("field");
                            for (int k = 0; k < fieldNodeList.getLength(); k++) {
                                Element fieldElement = (Element) fieldNodeList.item(k);
                                String fieldName = fieldElement.getTextContent();
                                for (int l = 0; l < columnList.getLength(); l++) {
                                    Element columnElement = (Element) columnList.item(l);
                                    Folder parentFolder = folder1;
                                    if (columnElement.getAttribute("name").equals(fieldName)) {
                                        columnCounter.add(fieldName);
                                        processField(columnElement, OWNER_TABLE_NAME, parentFolder, businessLayerFactory,
                                                businessLayerView);
                                    }
                                }
                            }

                        }

                    }
                }

            }

            if (columnList.getLength() > columnCounter.size()) {

                for (int j = 0; j < columnList.getLength(); j++) {
                    Element columnElement = (Element) columnList.item(j);
                    int needToHandleOrphanField = 1;
                    for (int k = 0; k < columnCounter.size(); k++) {
                        if (columnElement.getAttribute("name").equals(columnCounter.get(k))) {
                            needToHandleOrphanField = 0;
                            break;
                        }
                    }
                    if (needToHandleOrphanField == 1) { // This column was not handled before , Lets
                        // handle it now and add it under root
                        // folder

                        columnCounter.add(columnElement.getAttribute("name"));
                        processField(columnElement, OWNER_TABLE_NAME, rootFolder, businessLayerFactory, businessLayerView);

                    }

                }

            }

            // Hiding the Master view since we enabled a Personal View
            logger.info("Hiding Master Business Layer View");
            businessLayer.setMasterViewHidden(true);

            // Saves the business layer
            logger.info("Saving Business Layer in Local Folder " + LOCAL_FOLDER + BL_NAME + ".blx");
            localResourceService.save(businessLayer, LOCAL_FOLDER + BL_NAME + ".blx", true);

            //
            // ** Universe Publication to BOBJ CMS Repository
            //

            logger.info("Publishing Universe to BOBJ CMS " + CMS_NAME + " As " + BL_NAME + ".unx " + "In the Folder "
                    + CmsResourceService.UNIVERSES_ROOT);
            cmsResourceService.publish(LOCAL_FOLDER + BL_NAME + ".blx", CmsResourceService.UNIVERSES_ROOT, true);

            // ** Releases the loaded resources

            logger.info("Releasing all loaded resources");
            localResourceService.close(businessLayer);
            localResourceService.close(dataFoundation);
            localResourceService.close(connection);

        }
        // Release Session and Context
        context.close();
        enterpriseSession.logoff();

    }

    // Add a Folder into business Service Layer
    private static Folder addFolder(String folderName, String description, Folder parentFolder,
                                    BusinessLayerFactory businessLayerFactory, BusinessLayerView businessLayerView) {

        logger.info("Creating Folder " + folderName);
        Folder Folder = businessLayerFactory.createBlItem(Folder.class, folderName.trim(), parentFolder);
        Folder.setDescription(description);
        Folder.setState(ItemState.ACTIVE);
        businessLayerView.getObjects().add(Folder);
        return Folder;
    }

    // Add a Dimension into business Service Layer
    private static void addDimension(Element columnElement, String OWNER_TABLE_NAME, Folder parentFolder,
                                     BusinessLayerFactory businessLayerFactory, BusinessLayerView businessLayerView) {

        // Creates a measure in the business layer
        String columnName = columnElement.getAttribute("name").replace("[", "").replaceAll("]", "").trim();
        String columnCaption = columnElement.getAttribute("caption").trim();
        String columnDataType = columnElement.getAttribute("datatype");
        logger.info("Creating Dimension \"" + columnCaption + "\"");
        Dimension blxDimension1 = businessLayerFactory.createBlItem(Dimension.class, columnCaption, parentFolder);
        blxDimension1.setDescription(columnCaption);
        blxDimension1.setAccessLevel(AccessLevel.PUBLIC);
        blxDimension1.setDataType(resolveUniverseDataType(columnDataType));
        blxDimension1.setState(ItemState.ACTIVE);
        RelationalBinding binding = (RelationalBinding) blxDimension1.getBinding();
        binding.setSelect(OWNER_TABLE_NAME + "." + "`" + columnName + "`");
        businessLayerView.getObjects().add(blxDimension1);

    }

    // Add a measure into business Service Layer
    private static void addMeasure(Element columnElement, String OWNER_TABLE_NAME, Folder parentFolder,
                                   BusinessLayerFactory businessLayerFactory, BusinessLayerView businessLayerView) {

        String columnName = columnElement.getAttribute("name").replace("[", "").replaceAll("]", "").trim();
        String columnCaption = columnElement.getAttribute("caption").trim();
        String columnDataType = columnElement.getAttribute("datatype");
        String columnAggregation = columnElement.getAttribute("aggregation");
        logger.info("Creating measure \"" + columnCaption + "\"");
        Measure blxMeasure1 = businessLayerFactory.createBlItem(Measure.class, columnCaption, parentFolder);
        blxMeasure1.setDescription("Aggregation used on this Measure in the Cube - " + columnAggregation);
        blxMeasure1.setAccessLevel(AccessLevel.PUBLIC);
        blxMeasure1.setDataType(resolveUniverseDataType(columnDataType));
        blxMeasure1.setState(ItemState.ACTIVE);
        blxMeasure1.setProjectionFunction(resolveUniverseProjectionFunction(columnAggregation));
        RelationalBinding binding = (RelationalBinding) blxMeasure1.getBinding();
        binding.setSelect(OWNER_TABLE_NAME + "." + "`" + columnName + "`");
        businessLayerView.getObjects().add(blxMeasure1);
    }

    private static DataType resolveUniverseDataType(String columnDataType) {

        DataType unvDataType = DataType.NUMERIC;
        switch (columnDataType) {
            case "string":
                unvDataType = DataType.STRING;
                break;
            case "integer":
                unvDataType = DataType.NUMERIC;
                break;
            case "real":
                unvDataType = DataType.NUMERIC;
                break;
            case "date":
                unvDataType = DataType.DATE;
                break;
            case "datetime":
                unvDataType = DataType.DATE_TIME;
                break;
            case "boolean":
                unvDataType = DataType.BOOLEAN;
                break;
            default:
                unvDataType = DataType.STRING;
                break;
        }
        return unvDataType;
    }

    private static ProjectionFunction resolveUniverseProjectionFunction(String columnAggregation) {

        ProjectionFunction univPF = ProjectionFunction.SUM;
        switch (columnAggregation) {
            case "Sum":
                univPF = ProjectionFunction.SUM;
                break;
            case "CountD":
                univPF = ProjectionFunction.COUNT;
                break;
            case "Avg":
                univPF = ProjectionFunction.AVERAGE;
                break;
            case "Min":
                univPF = ProjectionFunction.MIN;
                break;
            case "Max":
                univPF = ProjectionFunction.MAX;
                break;
            default:
                univPF = ProjectionFunction.SUM;
                break;
        }
        return univPF;
    }



    private static void processField(Element columnElement, String OWNER_TABLE_NAME, Folder parentFolder,
                                     BusinessLayerFactory businessLayerFactory, BusinessLayerView businessLayerView) {
        if (columnElement.getAttribute("role").equals("measure")) {
            // Creates a measure in
            // the business layer
            addMeasure(columnElement, OWNER_TABLE_NAME, parentFolder, businessLayerFactory, businessLayerView);
        } else {
            // Creates a measure in
            // the business layer
            addDimension(columnElement, OWNER_TABLE_NAME, parentFolder, businessLayerFactory, businessLayerView);
        }

    }
}
