import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.poi.openxml4j.exceptions.InvalidOperationException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFTable;
import org.apache.poi.xwpf.usermodel.XWPFTableCell;
import org.apache.poi.xwpf.usermodel.XWPFTableRow;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.util.CellAddress;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.util.SAXHelper;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler.SheetContentsHandler;
import org.apache.poi.xssf.usermodel.XSSFComment;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import org.apache.poi.hssf.eventusermodel.EventWorkbookBuilder.SheetRecordCollectingListener;
import org.apache.poi.hssf.eventusermodel.FormatTrackingHSSFListener;
import org.apache.poi.hssf.eventusermodel.HSSFEventFactory;
import org.apache.poi.hssf.eventusermodel.HSSFListener;
import org.apache.poi.hssf.eventusermodel.HSSFRequest;
import org.apache.poi.hssf.eventusermodel.MissingRecordAwareHSSFListener;
import org.apache.poi.hssf.eventusermodel.dummyrecord.LastCellOfRowDummyRecord;
import org.apache.poi.hssf.eventusermodel.dummyrecord.MissingCellDummyRecord;
import org.apache.poi.hssf.model.HSSFFormulaParser;
import org.apache.poi.hssf.record.BOFRecord;
import org.apache.poi.hssf.record.BlankRecord;
import org.apache.poi.hssf.record.BoolErrRecord;
import org.apache.poi.hssf.record.BoundSheetRecord;
import org.apache.poi.hssf.record.FormulaRecord;
import org.apache.poi.hssf.record.LabelRecord;
import org.apache.poi.hssf.record.LabelSSTRecord;
import org.apache.poi.hssf.record.NoteRecord;
import org.apache.poi.hssf.record.NumberRecord;
import org.apache.poi.hssf.record.RKRecord;
import org.apache.poi.hssf.record.Record;
import org.apache.poi.hssf.record.SSTRecord;
import org.apache.poi.hssf.record.StringRecord;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.RDFNode;

import virtuoso.jena.driver.*;

public class PopulateAIRelation {

	private static final String graphName = "http://localhost:8890/cisco5_MT";
	private static final String url = "jdbc:virtuoso://localhost:1111";
	private static final String user = "dba";
	private static final String password = "dba";
	private static VirtGraph graph = null;
	
	public static String DELIMITER_COMMA = ",";

	public static VirtGraph initGraph() {
		if(graph==null) {
			graph = new VirtGraph (graphName,url,user,password);
		}
		return graph;
	}
	
	public static Node getNodeByName(String name) {

		initGraph();
		
		String query = "PREFIX : <> SELECT * WHERE { VALUES (?p ?o) {(:name \""+name+"\")} ?s ?p ?o }";

		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, graph);

		ResultSet results = vqe.execSelect();
		boolean isExist = false;
		Node node = new NodeImpl();
	    node.setName(name);
		while (results.hasNext()) {
			isExist = true;
			QuerySolution result = results.nextSolution();
		    RDFNode s = result.get("s");
		    RDFNode p = result.get("p");
		    RDFNode o = result.get("o");
		    node.setId(s.toString());
		    node.setProperty(p.toString(),replaceDoubleQuotes(o.toString()));
//		    System.out.println("{ " + s + " " + p + " " + o + " . }");
		}
	    if(isExist) {
//		    ExtendedIterator<Triple> triples = graph.find(NodeFactory.createURI(node.getId()), org.apache.jena.graph.Node.ANY, org.apache.jena.graph.Node.ANY);
//		    while(triples.hasNext()) {
//		    	Triple triple = triples.next();
//		    	org.apache.jena.graph.Node predicate = triple.getPredicate();
//		    	org.apache.jena.graph.Node object = triple.getObject();
//		    	if(!object.toString().contains("nodeID:")) {
//				    node.setProperty(predicate.toString(),replaceDoubleQuotes(object.toString()));		    		
//		    	}
//		    }
//		    System.out.println("Completed node = "+node);
		    vqe.close();

	    	return node;
	    }else {
		    vqe.close();

		    return null;	    	
	    }
	}

	public static Node getNodeById(String id) {

		initGraph();

		String modifiedId = id.split("nodeID://")[1];
		String query = "PREFIX nodeID: <nodeID://> PREFIX : <> SELECT * WHERE { VALUES (?s ?p) {(nodeID:"+modifiedId+" :name)} ?s ?p ?o }";

		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, graph);

		ResultSet results = vqe.execSelect();
		boolean isExist = false;
		Node node = new NodeImpl();
	    node.setId(id);
		while (results.hasNext()) {
			isExist = true;
			QuerySolution result = results.nextSolution();
		    RDFNode s = result.get("s");
		    RDFNode p = result.get("p");
		    RDFNode o = result.get("o");
		    node.setName(o.toString());
		    node.setProperty(p.toString(),replaceDoubleQuotes(o.toString()));
//		    System.out.println("{ " + s + " " + p + " " + o + " . }");
		}
	    if(isExist) {
//		    ExtendedIterator<Triple> triples = graph.find(NodeFactory.createURI(node.getId()), org.apache.jena.graph.Node.ANY, org.apache.jena.graph.Node.ANY);
//		    while(triples.hasNext()) {
//		    	Triple triple = triples.next();
//		    	org.apache.jena.graph.Node predicate = triple.getPredicate();
//		    	org.apache.jena.graph.Node object = triple.getObject();
//		    	if(!object.toString().contains("nodeID:")) {
//				    node.setProperty(predicate.toString(),replaceDoubleQuotes(object.toString()));		    		
//		    	}
//		    }
//		    System.out.println("Completed node = "+node);
		    vqe.close();

	    	return node;
	    }else {
		    vqe.close();

		    return null;	    	
	    }
	}

	public static String createNode(String name) {

		initGraph();

		String varName = getHash(name);
        String query = "INSERT { _:"+varName+" <name> \""+name+"\" . }";

		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, graph);
		vqe.execConstruct();

		query = "PREFIX : <> SELECT * WHERE { VALUES (?p ?o) {(:name \""+name+"\")} ?s ?p ?o }";

		vqe = VirtuosoQueryExecutionFactory.create(query, graph);

		ResultSet results = vqe.execSelect();
		String nodeId = null;
		while (results.hasNext()) {
			QuerySolution result = results.nextSolution();
		    RDFNode s = result.get("s");
		    RDFNode p = result.get("p");
		    RDFNode o = result.get("o");
		    
		    nodeId = s.toString();
		    System.out.println("{ " + s + " " + p + " " + o + " . }");
		}
		
	    vqe.close();

		return nodeId;
	}

	public static String createRelation(String startNodeId, String relationName, String endNodeId) {

		initGraph();

		if(!relationName.contains("##")) {
			relationName = relationName.replace(" ", "##");
			relationName = "###" + relationName;
		}

		org.apache.jena.graph.Node snode = NodeFactory.createURI(startNodeId);
		org.apache.jena.graph.Node pnode = NodeFactory.createURI(relationName);
		org.apache.jena.graph.Node onode = NodeFactory.createURI(endNodeId);

		graph.add(new Triple(snode,pnode,onode));
		graph.add(new Triple(onode,pnode,snode));
		
		return startNodeId+"|"+relationName+"|"+endNodeId;
	}

	public static void createRelationInBatch(List<Relation> relationList) {

		initGraph();

		int processedRelationCount = 0;
		if(relationList!=null && !relationList.isEmpty()) {
			System.out.println("Started Creating relations, total relations = "+relationList.size());
			String prefix = "PREFIX nodeID: <nodeID://> ";
			String insert = "INSERT { ";
			for(Relation relation : relationList) {
				String startNodeId = relation.getStartNode().getId().split("//")[1];
				String relationName = relation.getRelationType().getId();
				String endNodeId = relation.getEndNode().getId().split("//")[1];
				
				if(!relationName.contains("##")) {
					relationName = relationName.replace(" ", "##");
					relationName = "###" + relationName;
				}
				
				insert = insert + "nodeID:"+startNodeId+" <"+relationName+"> nodeID:"+endNodeId+" . ";
				
				processedRelationCount++;
				if(processedRelationCount%1000==0) {
					String query = prefix + insert + " }";
					VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, graph);
					vqe.execConstruct();	
					vqe.close();
					
					System.out.println("Completed Creating 1000 relations, total relations processed = "+processedRelationCount);
					prefix = "PREFIX nodeID: <nodeID://> ";
					insert = "INSERT { ";					
				}
			}

			if(!insert.equalsIgnoreCase("INSERT { ")) {
				String query = prefix + insert + " }";
				VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, graph);
				vqe.execConstruct();
				vqe.close();
			}
			System.out.println("Completed Creating relations, total relations processed = "+processedRelationCount);
		}
	}

	public static NodeSet getNodeSet(String startNodeId, String relationName, String endNodeName) {

		initGraph();

		String relationNamePrefix = "";
		if(!relationName.contains("##")) {
			relationName = relationName.replace(" ", "##");
			relationName = "###" + relationName;
		}
		if(relationName.contains("##")) {
			relationNamePrefix = relationName.substring(0,relationName.lastIndexOf("##")+2);
			relationName = relationName.substring(relationName.lastIndexOf("##")+2,relationName.length());
		}
    	String id = startNodeId.split("//")[1];
		String query = "PREFIX nodeID: <nodeID://> PREFIX : <"+relationNamePrefix+"> SELECT * WHERE { VALUES (?p ) {(:"+relationName+" )} nodeID:"+id+" ?p ?o }";
		
		if(endNodeName!=null && !endNodeName.isEmpty()) {
			String isNodeTypeRelationName = "Is NodeType";
			isNodeTypeRelationName = isNodeTypeRelationName.replace(" ", "##");
			isNodeTypeRelationName = "###" + isNodeTypeRelationName;
			String isNodeTypeRelationNamePrefix = "";
			if(isNodeTypeRelationName.contains("##")) {
				isNodeTypeRelationNamePrefix = isNodeTypeRelationName.substring(0,isNodeTypeRelationName.lastIndexOf("##")+2);
				isNodeTypeRelationName = isNodeTypeRelationName.substring(isNodeTypeRelationName.lastIndexOf("##")+2,isNodeTypeRelationName.length());
			}
			String endNodeId = getNodeByName(endNodeName).getId().split("//")[1];
			query = "PREFIX nodeID: <nodeID://> PREFIX : <"+relationNamePrefix+"> PREFIX isNodeType: <"+isNodeTypeRelationNamePrefix+"> SELECT * WHERE { VALUES (?p ) {(:"+relationName+" )} nodeID:"+id+" ?p ?o FILTER ( EXISTS { ?o isNodeType:"+isNodeTypeRelationName+" nodeID:"+endNodeId+" }) }";
		}

		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, graph);

		ResultSet results = vqe.execSelect();
	    Set<String> graphNodeSet = new HashSet<String>();
		while (results.hasNext()) {
			QuerySolution result = results.nextSolution();
		    RDFNode s = result.get("s");
		    RDFNode p = result.get("p");
		    RDFNode o = result.get("o");
		    
	    	if(o.toString().contains("nodeID:")) {
	    		graphNodeSet.add(o.toString());
	    	}	
		}

//		if(endNodeName!=null && !endNodeName.isEmpty()) {			
//		    NodeSet modifiedNodeSet = new NodeSetImpl();
//			NodeSet endNodeSet = isNodeTypeIncoming(getNodeByName(endNodeName).getId());
//			if(endNodeSet!=null && endNodeSet.getNodeList()!=null) {
//				List<String> nodeIdList = endNodeSet.getNodeIdList();
//				for(String nodeId : graphNodeSet) {
//					if(nodeIdList.contains(nodeId)) {
//						modifiedNodeSet.addNode(endNodeSet.getNodeById(nodeId));
//					}
//				}
//			}
//			return modifiedNodeSet;
//		}
		
	    NodeSet nodeSet = new NodeSetImpl();
	    for(String nodeId : graphNodeSet) {
	    	id = nodeId.split("//")[1];
			query = "PREFIX nodeID: <nodeID://> PREFIX : <> SELECT * WHERE { VALUES (?p ) {(:name )} nodeID:"+id+" ?p ?o }";

			vqe = VirtuosoQueryExecutionFactory.create(query, graph);

			results = vqe.execSelect();
			while (results.hasNext()) {
				QuerySolution result = results.nextSolution();
			    RDFNode s = result.get("s");
			    RDFNode p = result.get("p");
			    RDFNode o = result.get("o");
			    
		    	if(p.toString().equals("name")) {
		    		nodeSet.addNode(new NodeImpl(nodeId, o.toString()));
		    		break;
		    	}	
			}
			vqe.close();
	    }

	    return nodeSet;
	}

	public static List<RelationType> getRelationTypeList(){
		List<RelationType> relationTypeList = new ArrayList<RelationType>();
		
		initGraph();

		Node relationTypeNode = getNodeByName("RelationType");
		String relationName = "Is A";
		relationName = relationName.replace(" ", "##");
		relationName = "###" + relationName;
		String relationNamePrefix = "";
		if(relationName.contains("##")) {
			relationNamePrefix = relationName.substring(0,relationName.lastIndexOf("##")+2);
			relationName = relationName.substring(relationName.lastIndexOf("##")+2,relationName.length());
		}
    	String id = relationTypeNode.getId().split("//")[1];
		String query = "PREFIX nodeID: <nodeID://> PREFIX : <"+relationNamePrefix+"> SELECT * WHERE { VALUES (?p ) {(:"+relationName+" )} ?s ?p nodeID:"+id+" }";

		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, graph);

		ResultSet results = vqe.execSelect();
	    Set<String> graphNodeSet = new HashSet<String>();
		while (results.hasNext()) {
			QuerySolution result = results.nextSolution();
		    RDFNode s = result.get("s");
		    RDFNode p = result.get("p");
		    RDFNode o = result.get("o");
		    
	    	if(s.toString().contains("nodeID:")) {
	    		graphNodeSet.add(s.toString());
	    	}	
		}

	    for(String nodeId : graphNodeSet) {
	    	id = nodeId.split("//")[1];
			query = "PREFIX nodeID: <nodeID://> PREFIX : <> SELECT * WHERE { VALUES (?p ) {(:name )} nodeID:"+id+" ?p ?o }";

			vqe = VirtuosoQueryExecutionFactory.create(query, graph);

			results = vqe.execSelect();
			while (results.hasNext()) {
				QuerySolution result = results.nextSolution();
			    RDFNode s = result.get("s");
			    RDFNode p = result.get("p");
			    RDFNode o = result.get("o");
			    
		    	if(p.toString().equals("name")) {
		    		relationTypeList.add(new RelationTypeImpl(nodeId,replaceDoubleQuotes(o.toString())));
		    		break;
		    	}	
			}
		    vqe.close();
	    }

	    vqe.close();

		return relationTypeList;
	}

	public static String createNodeWithType(Node node, String type) {
		initGraph();

		String relationName = "Is NodeType";
		relationName = relationName.replace(" ", "##");
		relationName = "###" + relationName;
		
		Node typeNode = getNodeByName(type);
		String id = typeNode.getId().split("//")[1];
		
		String name = node.getName();
		String varName = getHash(name);
        String query = "PREFIX nodeID: <nodeID://> INSERT { _:"+varName+" <name> \""+name+"\" . _:"+varName+" <"+relationName+"> nodeID:"+id+" . nodeID:"+id+" <"+relationName+"> _:"+varName+" . }";

		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, graph);
		vqe.execConstruct();

		query = "PREFIX : <> SELECT * WHERE { VALUES (?p ?o) {(:name \""+name+"\")} ?s ?p ?o }";

		vqe = VirtuosoQueryExecutionFactory.create(query, graph);

		ResultSet results = vqe.execSelect();
		String nodeId = null;
		while (results.hasNext()) {
			QuerySolution result = results.nextSolution();
		    RDFNode s = result.get("s");
		    RDFNode p = result.get("p");
		    RDFNode o = result.get("o");
		    
		    nodeId = s.toString();
//		    System.out.println("{ " + s + " " + p + " " + o + " . }");
		}
		
	    vqe.close();

		return nodeId;
	}

	public static String getRelationId(String startNodeId, String relationName, String endNodeId) {
		
		initGraph();
		
		String rlName = relationName;
		
		if(!relationName.contains("##")) {
			relationName = relationName.replace(" ", "##");
			relationName = "###" + relationName;
		}

		String relationNamePrefix = "";
		if(relationName.contains("##")) {
			relationNamePrefix = relationName.substring(0,relationName.lastIndexOf("##")+2);
			relationName = relationName.substring(relationName.lastIndexOf("##")+2,relationName.length());
		}
    	String id1 = startNodeId.split("//")[1];
    	String id2 = endNodeId.split("//")[1];
		String query = "PREFIX nodeID: <nodeID://> PREFIX : <"+relationNamePrefix+"> SELECT * WHERE { VALUES (?p ) {(:"+relationName+" )} nodeID:"+id1+" ?p nodeID:"+id2+" }";
		
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, graph);
		vqe = VirtuosoQueryExecutionFactory.create(query, graph);

		ResultSet results = vqe.execSelect();
		while (results.hasNext()) {
			return startNodeId+"|"+relationName+"|"+endNodeId;
		}

    	id1 = endNodeId.split("//")[1];
    	id2 = startNodeId.split("//")[1];
		query = "PREFIX nodeID: <nodeID://> PREFIX : <"+relationNamePrefix+"> SELECT * WHERE { VALUES (?p ) {(:"+relationName+" )} nodeID:"+id1+" ?p nodeID:"+id2+" }";
		
		vqe = VirtuosoQueryExecutionFactory.create(query, graph);
		vqe = VirtuosoQueryExecutionFactory.create(query, graph);

		results = vqe.execSelect();
		while (results.hasNext()) {
		    vqe.close();

			return endNodeId+"|"+rlName+"|"+startNodeId;
		}

	    vqe.close();

		return null;
	}

	public static String createRelationType(String name) {

		initGraph();

		String relationName = "Is A";
		relationName = relationName.replace(" ", "##");
		relationName = "###" + relationName;
		
		Node typeNode = getNodeByName("RelationType");
		String id = typeNode.getId().split("//")[1];
		
		String varName = getHash(name);
        String query = "PREFIX nodeID: <nodeID://> INSERT { _:"+varName+" <name> \""+name+"\" . _:"+varName+" <"+relationName+"> nodeID:"+id+" . nodeID:"+id+" <"+relationName+"> _:"+varName+" . }";

		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, graph);
		vqe.execConstruct();

		query = "PREFIX : <> SELECT * WHERE { VALUES (?p ?o) {(:name \""+name+"\")} ?s ?p ?o }";

		vqe = VirtuosoQueryExecutionFactory.create(query, graph);

		ResultSet results = vqe.execSelect();
		String nodeId = null;
		while (results.hasNext()) {
			QuerySolution result = results.nextSolution();
		    RDFNode s = result.get("s");
		    RDFNode p = result.get("p");
		    RDFNode o = result.get("o");
		    
		    nodeId = s.toString();
		    System.out.println("{ " + s + " " + p + " " + o + " . }");
		}
		
	    vqe.close();

		return nodeId;
	}

	public static NodeSet isAIncoming(String nodeId){
		NodeSet nodeSet = new NodeSetImpl();
		
		initGraph();

		String relationName = "Is A";
		relationName = relationName.replace(" ", "##");
		relationName = "###" + relationName;
		String relationNamePrefix = "";
		if(relationName.contains("##")) {
			relationNamePrefix = relationName.substring(0,relationName.lastIndexOf("##")+2);
			relationName = relationName.substring(relationName.lastIndexOf("##")+2,relationName.length());
		}
    	String id = nodeId.split("//")[1];
		String query = "PREFIX nodeID: <nodeID://> PREFIX : <"+relationNamePrefix+"> SELECT * WHERE { VALUES (?p ) {(:"+relationName+" )} ?s ?p nodeID:"+id+" }";

		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, graph);

		ResultSet results = vqe.execSelect();
	    Set<String> graphNodeSet = new HashSet<String>();
		while (results.hasNext()) {
			QuerySolution result = results.nextSolution();
		    RDFNode s = result.get("s");
		    RDFNode p = result.get("p");
		    RDFNode o = result.get("o");
		    
	    	if(s.toString().contains("nodeID:")) {
	    		graphNodeSet.add(s.toString());
	    	}	
		}

	    for(String graphNodeId : graphNodeSet) {
	    	id = graphNodeId.split("//")[1];
			query = "PREFIX nodeID: <nodeID://> PREFIX : <> SELECT * WHERE { VALUES (?p ) {(:name )} nodeID:"+id+" ?p ?o }";

			vqe = VirtuosoQueryExecutionFactory.create(query, graph);

			results = vqe.execSelect();
			while (results.hasNext()) {
				QuerySolution result = results.nextSolution();
			    RDFNode s = result.get("s");
			    RDFNode p = result.get("p");
			    RDFNode o = result.get("o");
			    
		    	if(p.toString().equals("name")) {
		    		nodeSet.addNode(new NodeImpl(graphNodeId,replaceDoubleQuotes(o.toString())));
		    		break;
		    	}	
			}
		    vqe.close();
	    }

	    vqe.close();

		return nodeSet;
	}

	public static NodeSet isAOutgoing(String nodeId){
		NodeSet nodeSet = new NodeSetImpl();
		
		initGraph();

		String relationName = "Is A";
		relationName = relationName.replace(" ", "##");
		relationName = "###" + relationName;
		String relationNamePrefix = "";
		if(relationName.contains("##")) {
			relationNamePrefix = relationName.substring(0,relationName.lastIndexOf("##")+2);
			relationName = relationName.substring(relationName.lastIndexOf("##")+2,relationName.length());
		}
    	String id = nodeId.split("//")[1];
		String query = "PREFIX nodeID: <nodeID://> PREFIX : <"+relationNamePrefix+"> SELECT * WHERE { VALUES (?p ) {(:"+relationName+" )} nodeID:"+id+" ?p ?o }";

		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, graph);

		ResultSet results = vqe.execSelect();
	    Set<String> graphNodeSet = new HashSet<String>();
		while (results.hasNext()) {
			QuerySolution result = results.nextSolution();
		    RDFNode s = result.get("s");
		    RDFNode p = result.get("p");
		    RDFNode o = result.get("o");
		    
	    	if(o.toString().contains("nodeID:")) {
	    		graphNodeSet.add(o.toString());
	    	}	
		}

	    for(String graphNodeId : graphNodeSet) {
	    	id = graphNodeId.split("//")[1];
			query = "PREFIX nodeID: <nodeID://> PREFIX : <> SELECT * WHERE { VALUES (?p ) {(:name )} nodeID:"+id+" ?p ?o }";

			vqe = VirtuosoQueryExecutionFactory.create(query, graph);

			results = vqe.execSelect();
			while (results.hasNext()) {
				QuerySolution result = results.nextSolution();
			    RDFNode s = result.get("s");
			    RDFNode p = result.get("p");
			    RDFNode o = result.get("o");
			    
		    	if(p.toString().equals("name")) {
		    		nodeSet.addNode(new NodeImpl(graphNodeId,replaceDoubleQuotes(o.toString())));
		    		break;
		    	}	
			}
		    vqe.close();
	    }

	    vqe.close();

		return nodeSet;
	}

	public static NodeSet isNodeTypeIncoming(String nodeId){
		NodeSet nodeSet = new NodeSetImpl();
		
		initGraph();

		String relationName = "Is NodeType";
		relationName = relationName.replace(" ", "##");
		relationName = "###" + relationName;
		String relationNamePrefix = "";
		if(relationName.contains("##")) {
			relationNamePrefix = relationName.substring(0,relationName.lastIndexOf("##")+2);
			relationName = relationName.substring(relationName.lastIndexOf("##")+2,relationName.length());
		}
    	String id = nodeId.split("//")[1];
		String query = "PREFIX nodeID: <nodeID://> PREFIX : <"+relationNamePrefix+"> SELECT * WHERE { VALUES (?p ) {(:"+relationName+" )} ?s ?p nodeID:"+id+" }";

		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, graph);

		ResultSet results = vqe.execSelect();
	    Set<String> graphNodeSet = new HashSet<String>();
		while (results.hasNext()) {
			QuerySolution result = results.nextSolution();
		    RDFNode s = result.get("s");
		    RDFNode p = result.get("p");
		    RDFNode o = result.get("o");
		    
	    	if(s.toString().contains("nodeID:")) {
	    		graphNodeSet.add(s.toString());
	    	}	
		}

	    for(String graphNodeId : graphNodeSet) {
	    	id = graphNodeId.split("//")[1];
			query = "PREFIX nodeID: <nodeID://> PREFIX : <> SELECT * WHERE { VALUES (?p ) {(:name )} nodeID:"+id+" ?p ?o }";

			vqe = VirtuosoQueryExecutionFactory.create(query, graph);

			results = vqe.execSelect();
			while (results.hasNext()) {
				QuerySolution result = results.nextSolution();
			    RDFNode s = result.get("s");
			    RDFNode p = result.get("p");
			    RDFNode o = result.get("o");
			    
		    	if(p.toString().equals("name")) {
		    		nodeSet.addNode(new NodeImpl(graphNodeId,replaceDoubleQuotes(o.toString())));
		    		break;
		    	}	
			}
		    vqe.close();
	    }

	    vqe.close();

		return nodeSet;
	}

	public static NodeSet isNodeTypeOutgoing(String nodeId){
		NodeSet nodeSet = new NodeSetImpl();
		
		initGraph();

		String relationName = "Is NodeType";
		relationName = relationName.replace(" ", "##");
		relationName = "###" + relationName;
		String relationNamePrefix = "";
		if(relationName.contains("##")) {
			relationNamePrefix = relationName.substring(0,relationName.lastIndexOf("##")+2);
			relationName = relationName.substring(relationName.lastIndexOf("##")+2,relationName.length());
		}
    	String id = nodeId.split("//")[1];
		String query = "PREFIX nodeID: <nodeID://> PREFIX : <"+relationNamePrefix+"> SELECT * WHERE { VALUES (?p ) {(:"+relationName+" )} nodeID:"+id+" ?p ?o }";

		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, graph);

		ResultSet results = vqe.execSelect();
	    Set<String> graphNodeSet = new HashSet<String>();
		while (results.hasNext()) {
			QuerySolution result = results.nextSolution();
		    RDFNode s = result.get("s");
		    RDFNode p = result.get("p");
		    RDFNode o = result.get("o");
		    
	    	if(o.toString().contains("nodeID:")) {
	    		graphNodeSet.add(o.toString());
	    	}	
		}

	    for(String graphNodeId : graphNodeSet) {
	    	id = graphNodeId.split("//")[1];
			query = "PREFIX nodeID: <nodeID://> PREFIX : <> SELECT * WHERE { VALUES (?p ) {(:name )} nodeID:"+id+" ?p ?o }";

			vqe = VirtuosoQueryExecutionFactory.create(query, graph);

			results = vqe.execSelect();
			while (results.hasNext()) {
				QuerySolution result = results.nextSolution();
			    RDFNode s = result.get("s");
			    RDFNode p = result.get("p");
			    RDFNode o = result.get("o");
			    
		    	if(p.toString().equals("name")) {
		    		nodeSet.addNode(new NodeImpl(graphNodeId,replaceDoubleQuotes(o.toString())));
		    		break;
		    	}	
			}
		    vqe.close();
	    }

	    vqe.close();

	    return nodeSet;
	}

	public static String replaceDoubleQuotes(String value) {
		if(value!=null){
			if(value.charAt(0) == '"'){
				value = value.substring(1, value.length());
			}
			if(value.charAt(value.length()-1) == '"'){
				value = value.substring(0, value.length()-1);
			}								
		}else{
			value = "";
		}
		return value;
	}

	public static void createPaths(Map<String, String> nrnRelationMap) throws Exception{

		List<String> relationList = new ArrayList<String>();
		System.out.println("Going to get System Relation Types");
		List<RelationType> relationTypeList = getRelationTypeList();
		if(relationTypeList!=null){
			for(RelationType relationType : relationTypeList){
				relationList.add(relationType.getName().toLowerCase());							
			}
		}
		System.out.println("End of getting System Relation Types");

		if(!relationList.contains("hasDatasourceIDPaths".toLowerCase())) {
			createRelationType("hasDatasourceIDPaths");
		}
		
		File pathFolder = new File("D:\\uploadedFiles\\AIRelationPath");
		try {
			Map<String,String> pathKeyDatasouceMap = new HashMap<String,String>();
			Map<String,List<Node>> nodePairMap = new HashMap<String,List<Node>>();
			Map<String,String> relationMap = new HashMap<String,String>();
			List<String> pairIdList = new ArrayList<String>();
			List<String> nodeTypeIdList = new ArrayList<String>();
			Map<String,List<String>> combinationPathMap = new HashMap<String,List<String>>();
			if(pathFolder.listFiles()!=null && pathFolder.listFiles().length>0){
				File file = getLatestFileFromDirectory(pathFolder);
				if(file!=null){
					List<FileDataSet> fdsList = getFileDataSetSheetWiseList(file.getAbsolutePath(), "MSEXCEL", false, 0);
					if(fdsList!=null && !fdsList.isEmpty()){
						boolean isSuccess = false;
						boolean isSheetFound = false;
						for(FileDataSet fds : fdsList){
							List<String> headerList = fds.getHeaderNameList();
							if(headerList!=null && headerList.contains("Start Node") && headerList.contains("End Node") && headerList.contains("Path")){
								isSheetFound = true;
								List<Map<String,String>> sheetDataMap = fds.getDataMapList();
								isSuccess = true;//verifyData(graphService,sheetDataMap);
								if(isSuccess) {
									for(Map<String,String> rowMap : sheetDataMap){
										String startNodeName = rowMap.get("Start Node");
										String endNodeName = rowMap.get("End Node");
										String path = rowMap.get("Path");
										String forwardKeyDatasource = rowMap.get("Key Datasource");
										String reverseKeyDatasource = rowMap.get("Key Datasource (reverse)");
										if(forwardKeyDatasource!=null){
											forwardKeyDatasource = forwardKeyDatasource.trim();
										}
										if(reverseKeyDatasource!=null){
											reverseKeyDatasource = reverseKeyDatasource.trim();
										}
										
										if(startNodeName!=null && endNodeName!=null && !startNodeName.isEmpty() && !endNodeName.isEmpty() && path!=null && !path.isEmpty()){
											startNodeName = startNodeName.trim();
											endNodeName = endNodeName.trim();
											
//											if(startNodeName.contains(",")){
//												startNodeName = createIsNodeTypeRelations(graphService, startNodeName);
//											}
//											
//											if(endNodeName.contains(",")){
//												endNodeName = createIsNodeTypeRelations(graphService, endNodeName);
//											}
											
											String startNodeNameId = null;
											String endNodeNameId = null;

											Node nodeTypeNode = getNodeByName(startNodeName);
											if(nodeTypeNode!=null){
												startNodeNameId = nodeTypeNode.getId();
											}

											nodeTypeNode = getNodeByName(endNodeName);
											if(nodeTypeNode!=null){
												endNodeNameId = nodeTypeNode.getId();
											}

											if(!pairIdList.contains(startNodeNameId+"|"+endNodeNameId)){
												pairIdList.add(startNodeNameId+"|"+endNodeNameId);
												List<Node> pair = new ArrayList<Node>();
												pair.add(new NodeImpl(startNodeNameId,startNodeName));
												pair.add(new NodeImpl(endNodeNameId,endNodeName));
												nodePairMap.put(startNodeNameId+"|"+endNodeNameId, pair);
											}
											
											if(startNodeNameId!=null && endNodeNameId!=null){
												System.out.println("[Started] Creating path for "+startNodeName+" - "+endNodeName+" path = "+path);
												String nodeCombination = startNodeNameId + "|" + endNodeNameId;
												Node nodeCombinationNode = getNodeByName(nodeCombination);
												String nodeCombinationId = null;
												if(nodeCombinationNode!=null){
													nodeCombinationId = nodeCombinationNode.getId();
												}else{
													Node combinationNode = new NodeImpl();
													combinationNode.setName(nodeCombination);
													nodeCombinationId = createNodeWithType(combinationNode, "NodeCombination");
												}

												boolean isRelationFound = true;
												String modifiedPath = null;
												List<String> list = parseStrings(path, DELIMITER_COMMA);
												for(int i=0;i<list.size()-1;i++){
													String nodeName1 = list.get(i).trim();
													String nodeName2 = list.get(i+1).trim();
													
													String nodeId1 = null;
													String nodeId2 = null;

													Node nodeNameNode = getNodeByName(nodeName1);
													if(nodeNameNode!=null){
														nodeId1 = nodeNameNode.getId();
													}
													nodeNameNode = getNodeByName(nodeName2);
													if(nodeNameNode!=null){
														nodeId2 = nodeNameNode.getId();
													}

													if(!nodeTypeIdList.contains(nodeId1)){
														nodeTypeIdList.add(nodeId1);
													}
													if(!nodeTypeIdList.contains(nodeId2)){
														nodeTypeIdList.add(nodeId2);
													}

													String relationName = null;//relationMap.get(nodeId1+"|"+nodeId2);
													if(relationName==null){
														String relationId = getRelationId(nodeId1,"Has GraphRelation With",nodeId2);
														if(relationId!=null){
//															Relation dataContextRelation = graphService.getRelationById(relationId);
//															relationName = dataContextRelation.getProperty(Constants.RELATION_HASGRAPHRELATIONWITH_PROPERTY_RELATION_NAME);
															String relationNameFromFile = nrnRelationMap.get(nodeName1.toLowerCase()+"|"+nodeName2.toLowerCase());
															if(relationNameFromFile==null || relationNameFromFile.isEmpty()) {
																relationNameFromFile = nrnRelationMap.get(nodeName2.toLowerCase()+"|"+nodeName1.toLowerCase());												
															}
															relationName = relationNameFromFile;
															if(relationName!=null){
																relationMap.put(nodeId1+"|"+nodeId2, relationName);
																relationMap.put(nodeId2+"|"+nodeId1, relationName);																
															}else{
																System.out.println("No relation found between "+nodeName1+" and "+nodeName2+", hence skipping craeting path["+path+"] for "+startNodeName+" - "+endNodeName);
																String includesRelationId = getRelationId(nodeId1,"Includes",nodeId2);
																if(includesRelationId!=null){
																	relationName = "Is NodeType";													
																}else{
																	String aiRelationName = getProposedAIRelationName(nodeName1, nodeName2);//"AI_Has"+nodeName1+"-"+nodeName2;
																	if(!relationList.contains(aiRelationName.toLowerCase())){
																		createRelationType(aiRelationName);
																		relationList.add(aiRelationName.toLowerCase());
																	}
																	relationName = aiRelationName;
																}
															}
														}else{
															System.out.println("No relation found between "+nodeName1+" and "+nodeName2+", hence skipping craeting path["+path+"] for "+startNodeName+" - "+endNodeName);
															String includesRelationId = getRelationId(nodeId1,"Includes",nodeId2);
															if(includesRelationId!=null){
																relationName = "Is NodeType";													
															}else{
																String aiRelationName = getProposedAIRelationName(nodeName1, nodeName2);//"AI_Has"+nodeName1+"-"+nodeName2;
																if(!relationList.contains(aiRelationName.toLowerCase())){
																	createRelationType(aiRelationName);
																	relationList.add(aiRelationName.toLowerCase());
																}
																relationName = aiRelationName;
															}
														}										
													}
													
													if(relationName!=null){
														if(modifiedPath==null){
															modifiedPath = nodeId1 + "|" + relationName + "|" + nodeId2;
														}else{
															modifiedPath = modifiedPath + "|" + relationName + "|" + nodeId2;
														}
													}
												}	
												
												if(isRelationFound && modifiedPath!=null){
													Node existingPathNode = getNodeByName(modifiedPath);
													String pathId = null;
													if(existingPathNode!=null){
														pathId = existingPathNode.getId();
													}else{
														Node pathNode = new NodeImpl();
														pathNode.setName(modifiedPath);
														pathId = createNodeWithType(pathNode, "Graph Traversal Path");
													}

													if(pathId!=null && nodeCombinationId!=null){
														if(!combinationPathMap.containsKey(nodeCombinationId)) {
															combinationPathMap.put(nodeCombinationId, new ArrayList<String>());
														}
														combinationPathMap.get(nodeCombinationId).add(pathId);
														if(forwardKeyDatasource!=null && !forwardKeyDatasource.isEmpty()){
															pathKeyDatasouceMap.put(startNodeName+"|"+endNodeName+"|"+modifiedPath, forwardKeyDatasource);										
														}
													}
													
													
													String reverseModifiedPath = null;
													List<String> modifiedPathList = parseStrings(modifiedPath,"|");
													Collections.reverse(modifiedPathList);
													for(String reversePathToken : modifiedPathList){
														if(reverseModifiedPath==null){
															reverseModifiedPath = reversePathToken;
														}else{
															reverseModifiedPath = reverseModifiedPath + "|" + reversePathToken;
														}
													}

													System.out.println("[Started] Creating reverse path for "+startNodeName+" - "+endNodeName+" path = "+reverseModifiedPath);
													String reverseNodeCombination = endNodeNameId + "|" + startNodeNameId;
													Node reverseNodeCombinationNode = getNodeByName(reverseNodeCombination);
													String reverseNodeCombinationId = null;
													if(reverseNodeCombinationNode!=null){
														reverseNodeCombinationId = reverseNodeCombinationNode.getId();
													}else{
														Node combinationNode = new NodeImpl();
														combinationNode.setName(reverseNodeCombination);
														reverseNodeCombinationId = createNodeWithType(combinationNode, "NodeCombination");
													}

													Node reversePathNode = getNodeByName(reverseModifiedPath);
													String reversePathId = null;
													if(reversePathNode!=null){
														reversePathId = reversePathNode.getId();
													}else{
														Node pathNode = new NodeImpl();
														pathNode.setName(reverseModifiedPath);
														reversePathId = createNodeWithType(pathNode, "Graph Traversal Path");
													}

													if(reversePathId!=null && reverseNodeCombinationId!=null){
														if(!combinationPathMap.containsKey(reverseNodeCombinationId)) {
															combinationPathMap.put(reverseNodeCombinationId, new ArrayList<String>());
														}
														combinationPathMap.get(reverseNodeCombinationId).add(reversePathId);
														if(reverseKeyDatasource!=null && !reverseKeyDatasource.isEmpty()){
															pathKeyDatasouceMap.put(endNodeName+"|"+startNodeName+"|"+reverseModifiedPath, reverseKeyDatasource);										
														}
													}
													System.out.println("[Completed] Creating reverse path for "+startNodeName+" - "+endNodeName+" path = "+reverseModifiedPath);
												}
												System.out.println("[Completed] Creating path for "+startNodeName+" - "+endNodeName+" path = "+path);
											}
										}
									}									
								}
							}
						}
						
						if(isSheetFound){
							if(isSuccess) {	
								for(Map.Entry<String,List<String>> entry : combinationPathMap.entrySet()) {
									String combinationId = entry.getKey();
									List<String> pathIdList = entry.getValue();
									NodeSet nodeSet = getNodeSet(combinationId, "Has Graph Traversal Path", "Graph Traversal Path");
									if(nodeSet==null || nodeSet.getNodeList()==null){
										for(String pathId : pathIdList) {
											createRelation(combinationId,"Has Graph Traversal Path",pathId);										
										}										
									}
								}
								populateAllNodes_API(pairIdList, nodePairMap, pathKeyDatasouceMap,nodeTypeIdList);
							}else {
								System.out.println("Abnormal Condition detected : Unable to start populate nodes as some of the nodetypes have no node");
								System.out.println("Abnormal Condition detected : Unable to start populate nodes as some of the nodetypes have no node");
							}
						}else{
							System.out.println("File Structure does not match with the required structure. Need 3 columns with headers as 'Start Node', 'End Node' and 'Path'");
						}
					}	
				}
			}
			System.out.println("Completed Populating Nodes");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("PATH FILE NOT FOUND");
		}
	}
		
	public static void populateAllNodes_API(List<String> pairIdList, Map<String,List<Node>> nodePairMap, Map<String,String> pathKeyDatasouceMap, List<String> nodeTypeIdList){	
		try {
			if(pathKeyDatasouceMap!=null) {
				pathKeyDatasouceMap = Collections.unmodifiableMap(pathKeyDatasouceMap);				
			}

			Map<String,String> virtualIdMap = new HashMap<String,String>();

			System.out.println("Started populating nodeTypeIdNameMap");
			Map<String,String> nodeTypeIdNameMap = new HashMap<String,String>();
			NodeSet nodeTypeNodeSet = isAIncoming(getNodeByName("NodeType").getId());
			if(nodeTypeNodeSet!=null && nodeTypeNodeSet.getNodeList()!=null){
				for(Node nodeTypeNode : nodeTypeNodeSet.getNodeList()){
					nodeTypeIdNameMap.put(nodeTypeNode.getId(), nodeTypeNode.getName());
				}
				nodeTypeIdNameMap = Collections.unmodifiableMap(nodeTypeIdNameMap);
			}
			System.out.println("Completed populating nodeTypeIdNameMap");

			System.out.println("Started populating HashCodeIdCache");
//			populateHashCodeIdCache(graphService);
			System.out.println("Completed populating HashCodeIdCache");
			
			List<String> pivotNodeNameList = new ArrayList<String>();
			String systemEntityPivotNodeId = getNodeByName("PivotNode").getId();
			NodeSet pivotNodeSet = isAIncoming(systemEntityPivotNodeId);
			if(pivotNodeSet!=null && pivotNodeSet.getNodeList()!=null){
				pivotNodeNameList.addAll(pivotNodeSet.getNodeNameList());
			}
			
			List<String> relationList = new ArrayList<String>();
			System.out.println("Going to get System Relation Types");
			List<RelationType> relationTypeList = getRelationTypeList();
			if(relationTypeList!=null){
				for(RelationType relationType : relationTypeList){
					relationList.add(relationType.getName().toLowerCase());							
				}
			}
			System.out.println("End of getting System Relation Types");
			
			for(int i=0;i<pairIdList.size();i++){
				String pairId = pairIdList.get(i);
				List<Node> nodeList = nodePairMap.get(pairId);
				if(nodeList!=null){
					if(nodeList.size()==2){
						String nodeTypeName1 = nodeList.get(0).getName();
						String nodeTypeId1 = nodeList.get(0).getId();
						String nodeTypeName2 = nodeList.get(1).getName();
						String nodeTypeId2 = nodeList.get(1).getId();						

						if(nodeTypeId1!=null && nodeTypeId2!=null && nodeTypeName1!=null && nodeTypeName2!=null){			
							String relationName = getProposedAIRelationName(nodeTypeName1, nodeTypeName2);//"AI_Has"+nodeTypeName1+"-"+nodeTypeName2;
							if(!relationList.contains(relationName.toLowerCase())){
								createRelationType(relationName);
								relationList.add(relationName.toLowerCase());								
							}			

							Map<String, String> propertyMap = new HashMap<String, String>();
					        propertyMap.put("relationName", relationName);
							createRelation(nodeTypeId1,"hasMostUsedGraphRelationWith",nodeTypeId2);				

							System.out.println("[STARTED] pair = "+nodeTypeName1+" - "+nodeTypeName2);
							populateNodes_API(nodeTypeId1, nodeTypeName1, nodeTypeId2, nodeTypeName2, relationName, null, nodeTypeIdNameMap, pathKeyDatasouceMap, virtualIdMap);
							System.out.println("[COMPLETED] pair = "+nodeTypeName1+" - "+nodeTypeName2);
						}
					}
				}
			}
			pathKeyDatasouceMap=null;
			nodeTypeIdNameMap=null;
		}catch (Exception e) {	 
			String message = "Failed while processing populateAllNodes_API()";
			System.out.println(message);
			e.printStackTrace();
		}
	}
	
	public static String populateNodes_API(String nodeTypeId1, String nodeTypeName1, String nodeTypeId2, String nodeTypeName2, String relationName, String directRelationName, Map<String,String> nodeTypeIdNameMap,
			Map<String,String> pathKeyDatasouceMap, Map<String,String> virtualIdMap) throws Exception{
		Node nodeTypeNode1 = new NodeImpl(nodeTypeId1,nodeTypeName1);
		Node nodeTypeNode2 = new NodeImpl(nodeTypeId2,nodeTypeName2);

		Map<String,List<Node>> forwardMap = null;
		Map<String,List<Node>> reverseMap = null;
		Map<String,List<Node>> forwardDirectRelationMap = null;
		Map<String,List<Node>> reverseDirectRelationMap = null;
		forwardMap = new HashMap<String,List<Node>>();
		reverseMap = new HashMap<String,List<Node>>();
		forwardDirectRelationMap = new HashMap<String,List<Node>>();
		reverseDirectRelationMap = new HashMap<String,List<Node>>();			
		Map<String,List<String>> nodeCombinationDatasourcePathMap = new HashMap<String,List<String>>();
		Map<String,List<String>> nodeCombinationDatasourcePathIdMap = new HashMap<String,List<String>>();

		Map<String,String> masterNodeMap = new HashMap<String,String>();
		Map<String,NodeSet> masterNodeNodeSetMap = new HashMap<String,NodeSet>();
		NodeSet nodeNameNodeSet = getNodeSet(nodeTypeId1,"Is NodeType",null);
		if(nodeNameNodeSet!=null && nodeNameNodeSet.getNodeList()!=null){
			List<List<String>> allRoutesBetweenNodes = getDirectPaths_API(nodeTypeNode1,nodeTypeNode2,masterNodeMap,masterNodeNodeSetMap);
			System.out.println("Total Routes found ("+allRoutesBetweenNodes.size()+") = "+allRoutesBetweenNodes);
						
			System.out.println("[STARTED] traversing and creating Relation["+relationName+"] for total nodes = "+nodeNameNodeSet.getNodeList().size());

			int totalExecutedCount = 0;
			List<Node> finalNodeList = nodeNameNodeSet.getNodeList();
			for(Node node : finalNodeList){
				boolean isSkip = false;
				
				if(!isSkip) {
					TraverseAPICallableNew tcn = new TraverseAPICallableNew(node, nodeTypeNode1, nodeTypeNode2, allRoutesBetweenNodes, nodeTypeIdNameMap,
							pathKeyDatasouceMap,relationName,directRelationName,nodeCombinationDatasourcePathMap,nodeCombinationDatasourcePathIdMap);
					TraverseAPIDataNew data = tcn.call();
					if(data!=null) {
						if(data.forwardMap!=null) {
							forwardMap.putAll(data.forwardMap);
						}
						if(data.reverseMap!=null) {
							for(Map.Entry<String,List<Node>> e : data.reverseMap.entrySet()) {
								String key = e.getKey();
								List<Node> ndlList = e.getValue();
								List<Node> finalNdlList = new ArrayList<Node>();
								if(ndlList!=null) {
									finalNdlList.addAll(ndlList);											
								}
								if(reverseMap.get(key)!=null) {
									finalNdlList.addAll(reverseMap.get(key));
								}
								reverseMap.put(key,finalNdlList);
							}
						}
						if(data.forwardDirectRelationMap!=null) {
							forwardDirectRelationMap.putAll(data.forwardDirectRelationMap);
						}
						if(data.reverseDirectRelationMap!=null) {
							for(Map.Entry<String,List<Node>> e : data.reverseDirectRelationMap.entrySet()) {
								String key = e.getKey();
								List<Node> ndlList = e.getValue();
								List<Node> finalNdlList = new ArrayList<Node>();
								if(ndlList!=null) {
									finalNdlList.addAll(ndlList);											
								}
								if(reverseDirectRelationMap.get(key)!=null) {
									finalNdlList.addAll(reverseDirectRelationMap.get(key));
								}
								reverseDirectRelationMap.put(key,finalNdlList);
							}
						}
					}
					data=null;
					totalExecutedCount++;
					System.out.println("Pair "+nodeTypeName1+" - "+nodeTypeName2+", Total Executed = "+totalExecutedCount);
					System.out.println("TOTAL NODES EXECUTED = "+totalExecutedCount+" out of "+nodeNameNodeSet.getNodeList().size());
				}
			}
			
			System.out.println("Creating Forward Relations, map size : "+forwardMap.entrySet().size());
			List<Relation> forwardRelationList = new ArrayList<Relation>();
			for(Map.Entry<String,List<Node>> entry : forwardMap.entrySet())
			{
				String nodeId =  entry.getKey();
				List<Node> nodeList = entry.getValue();
				for(Node node : nodeList) {
					Relation relation = new RelationImpl();
					relation.setStartNodeId(nodeId);
					relation.setRelationTypeId(relationName);
					relation.setEndNodeId(node.getId());
					forwardRelationList.add(relation);
//					createRelation(nodeId, relationName, node.getId());					
				}
			}
			createRelationInBatch(forwardRelationList);
			
			System.out.println("Creating Reverse Relations, map size : "+reverseMap.entrySet().size());
			List<Relation> reverseRelationList = new ArrayList<Relation>();
			for(Map.Entry<String,List<Node>> entry : reverseMap.entrySet())
			{
				String nodeId =  entry.getKey();
				List<Node> nodeList = entry.getValue();
				for(Node node : nodeList) {
					Relation relation = new RelationImpl();
					relation.setStartNodeId(nodeId);
					relation.setRelationTypeId(relationName);
					relation.setEndNodeId(node.getId());
					reverseRelationList.add(relation);
//					createRelation(nodeId, relationName, node.getId());					
				}
			}
			createRelationInBatch(reverseRelationList);

			System.out.println("Started creating datasource paths size ="+nodeCombinationDatasourcePathMap.entrySet().size());
			List<Relation> dsPathRelationList = new ArrayList<Relation>();
			for(Map.Entry<String,List<String>> entry : nodeCombinationDatasourcePathMap.entrySet()){
				String nodeCombinationName = entry.getKey();
				List<String> datasourcePathList = entry.getValue();
				List<String> datasourcePathIdList = nodeCombinationDatasourcePathIdMap.get(entry.getKey());
				
				String nodeCombinationNodeId = null;
				Node existingNodeCombinationNode = getNodeByName(nodeCombinationName);
				if(existingNodeCombinationNode!=null){
					nodeCombinationNodeId = existingNodeCombinationNode.getId();
				}else{
					Node node = new NodeImpl();
					node.setName(nodeCombinationName);
					nodeCombinationNodeId = createNodeWithType(node, "NodeCombination");
				}
				for(String datasourcePath : datasourcePathList){
					String datasourcePathId = null;
					Node existingDatasourcePathNode = getNodeByName(datasourcePath);
					if(existingDatasourcePathNode!=null){
						datasourcePathId = existingDatasourcePathNode.getId();
					}else{
						Node node = new NodeImpl();
						node.setName(datasourcePath);
						datasourcePathId = createNodeWithType(node, "DatasourcePaths");
					}

					Relation relation = new RelationImpl();
					relation.setStartNodeId(nodeCombinationNodeId);
					relation.setRelationTypeId("hasDatasourcePaths");
					relation.setEndNodeId(datasourcePathId);
					dsPathRelationList.add(relation);
//					createRelation(nodeCombinationNodeId,"hasDatasourcePaths",datasourcePathId);						
				}
				for(String datasourcePath : datasourcePathIdList){
					String datasourcePathId = null;
					Node existingDatasourcePathNode = getNodeByName(datasourcePath);
					if(existingDatasourcePathNode!=null){
						datasourcePathId = existingDatasourcePathNode.getId();
					}else{
						Node node = new NodeImpl();
						node.setName(datasourcePath);
						datasourcePathId = createNodeWithType(node, "DatasourcePaths");
					}

					Relation relation = new RelationImpl();
					relation.setStartNodeId(nodeCombinationNodeId);
					relation.setRelationTypeId("hasDatasourcePaths");
					relation.setEndNodeId(datasourcePathId);
					dsPathRelationList.add(relation);
//					createRelation(nodeCombinationNodeId,"hasDatasourceIDPaths",datasourcePathId);						
				}
			}
			createRelationInBatch(dsPathRelationList);

			System.out.println("[COMPLETED] traversing and creating Relation["+relationName+"] for total nodes = "+nodeNameNodeSet.getNodeList().size());

			forwardMap = null;
			reverseMap = null;
			forwardDirectRelationMap = null;
			reverseDirectRelationMap = null;
			nodeCombinationDatasourcePathMap = null;
			forwardRelationList = null;
			reverseRelationList = null;
			dsPathRelationList = null;
		}
		return null;
	}

	public static List<List<String>> getDirectPaths_API(Node sourceNode, Node destinationNode, Map<String,String> masterNodeMap, Map<String,NodeSet> masterNodeNodeSetMap) throws Exception{
		Set<List<String>> finalModifiedRouteSet = new HashSet<List<String>>();
		String destinationNodeId = destinationNode.getId();
		String sourceNodeId = sourceNode.getId();


		boolean isRouteFound = false;
		String nodeCombination = sourceNodeId + "|" + destinationNodeId;
		Node nodeCombinationNode = getNodeByName(nodeCombination);
		if(nodeCombinationNode!=null){
			String nodeCombinationId = nodeCombinationNode.getId();
			NodeSet pathNodeSet = getNodeSet(nodeCombinationId, "Has Graph Traversal Path", "Graph Traversal Path");
			if(pathNodeSet!=null && pathNodeSet.getNodeList()!=null){
				isRouteFound = true;
				for(Node pathNode : pathNodeSet.getNodeList()){
					String path = pathNode.getName();
					List<String> list = parseStrings(path, "|");
					finalModifiedRouteSet.add(list);
				}
			}
		}
		
		if(isRouteFound){
			List<List<String>> finalRouteList = new ArrayList<List<String>>(finalModifiedRouteSet);
			return finalRouteList;
		}
		return null;
	}

	public static List<Node> traversal_API(Node startNode, Node sourceNodeTypeNode, Node destinationNodeTypeNode, List<List<String>> allRoutesBetweenNodes, Map<String,String> nodeTypeIdNameMap,
			Map<String, List<Node>> reverseNodeMap, Map<String, String> pathKeyDatasouceMap, Map<String,String> masterNodeMap, Map<String,NodeSet> masterNodeNodeSetMap,
			Map<String,List<String>> nodeCombinationDatasourcePathMap, Map<String,List<String>> nodeCombinationDatasourcePathIdsMap) throws Exception{
		List<Node> finalNodeList = new ArrayList<Node>();
		if(allRoutesBetweenNodes==null || allRoutesBetweenNodes.isEmpty()){
			allRoutesBetweenNodes = getDirectPaths_API(sourceNodeTypeNode,destinationNodeTypeNode,masterNodeMap,masterNodeNodeSetMap);
		}
		
		if(allRoutesBetweenNodes!=null && !allRoutesBetweenNodes.isEmpty()){
//			System.out.println("Total Routes found ("+allRoutesBetweenNodes.size()+") = "+allRoutesBetweenNodes);
			for(List<String> route : allRoutesBetweenNodes){
				List<Node> routeNodeList = new ArrayList<Node>();
				boolean isSkipRoute = false;
				if(route.size()==3){
					if(route.get(0).equals(sourceNodeTypeNode.getId()) && route.get(2).equals(destinationNodeTypeNode.getId())){
						isSkipRoute = true;
					}
				}
				if(!isSkipRoute){ // skipping direct NRN relations
//					System.out.println("[STARTED] route = "+route);
					
					Map<Node, List<String>> nodePathElementsMap = new HashMap<Node, List<String>>();
					Map<Node, List<String>> nodePathElementsIdMap = new HashMap<Node, List<String>>();
					Map<Node, Set<Node>> parentChildMap = new HashMap<Node, Set<Node>>();
					Map<String, Node> nodeToNodeTypeMap = new HashMap<String, Node>();

					for(int r=0;r<route.size()-2;r=r+2){
						String routeNode1Id = route.get(r);
						String routeRelationId = route.get(r+1);
						String routeNode2Id = route.get(r+2);
						String routeNode1Name = getNodeById(routeNode1Id).getName();
						String routeNode2Name = getNodeById(routeNode2Id).getName();
						List<Node> currentNodeList = new ArrayList<Node>();
//						A(NodeType)(DataSource)(DatasourceLocation)-Relation-Node1(NodeType)(DataSource)(DatasourceLocation)-Relation2-Node2(NodeType)(DataSource)(DatasourceLocation)-Relation3-Y(NodeType)(DataSource)(DatasourceLocation)
						if(routeRelationId!=null && !routeRelationId.isEmpty()){
							List<String> relationList = parseStrings(routeRelationId, DELIMITER_COMMA);
							for(String relationType : relationList){
								NodeSet nodeSet = null;
								if(r==0){
									nodeToNodeTypeMap.put(startNode.getId(), new NodeImpl(routeNode1Id,routeNode1Name));
									List<String> elementList = new ArrayList<String>();
									elementList.add("<span class='firsttext'>"+startNode.getName()+"</span>");
									elementList.add("(<span class='secondtext'>"+routeNode1Name+"</span>)");
									elementList.add("<br>-"+relationType+"-<br>");
									nodePathElementsMap.put(startNode, elementList);
									
									elementList = new ArrayList<String>();
									elementList.add("<span class='firsttext'>"+startNode.getId()+"</span>");
									elementList.add("(<span class='secondtext'>"+routeNode1Id+"</span>)");
									elementList.add("<br>-"+relationType+"-<br>");
									nodePathElementsIdMap.put(startNode, elementList);

									nodeSet = getNodeSet(startNode.getId(), relationType.trim(), routeNode2Name);
									if(nodeSet!=null && nodeSet.getNodeList()!=null){
										for(Node finalEndPoint : nodeSet.getNodeList()) {
											if(!parentChildMap.containsKey(startNode)) {
												parentChildMap.put(startNode, new HashSet<Node>());
											}
											parentChildMap.get(startNode).add(finalEndPoint);
											nodeToNodeTypeMap.put(finalEndPoint.getId(), new NodeImpl(routeNode2Id,routeNode2Name));
											List<String> elementList1 = new ArrayList<String>();
											elementList1.add("<span class='firsttext'>"+finalEndPoint.getName()+"</span>");
											elementList1.add("(<span class='secondtext'>"+routeNode2Name+"</span>)");
											nodePathElementsMap.put(finalEndPoint, elementList1);
											
											elementList1 = new ArrayList<String>();
											elementList1.add("<span class='firsttext'>"+finalEndPoint.getId()+"</span>");
											elementList1.add("(<span class='secondtext'>"+routeNode2Id+"</span>)");
											nodePathElementsIdMap.put(finalEndPoint, elementList1);
										}
									}
								}else{
									nodeSet = new NodeSetImpl();
									for(Node routeNode : routeNodeList){
										if(relationType.trim().equalsIgnoreCase("Is NodeType")){
											nodeSet.addNode(routeNode);
										}else{
											NodeSet nodeSetForCurrentRouteNode = getNodeSet(routeNode.getId(), relationType.trim(),routeNode2Name);
											if(nodeSetForCurrentRouteNode!=null && nodeSetForCurrentRouteNode.getNodeList()!=null){
												nodePathElementsMap.get(routeNode).add("<br>-"+relationType+"-<br>");
												nodePathElementsIdMap.get(routeNode).add("<br>-"+relationType+"-<br>");
												nodeSet.addAllNode(nodeSetForCurrentRouteNode.getNodeList());
												for(Node finalEndPoint : nodeSetForCurrentRouteNode.getNodeList()) {
													if(!parentChildMap.containsKey(routeNode)) {
														parentChildMap.put(routeNode, new HashSet<Node>());
													}
													parentChildMap.get(routeNode).add(finalEndPoint);
													
													nodeToNodeTypeMap.put(finalEndPoint.getId(), new NodeImpl(routeNode2Id,routeNode2Name));
													List<String> finalNodeElementList = new ArrayList<String>();
													finalNodeElementList.add("<span class='firsttext'>"+finalEndPoint.getName()+"</span>");
													finalNodeElementList.add("(<span class='secondtext'>"+routeNode2Name+"</span>)");
													nodePathElementsMap.put(finalEndPoint, finalNodeElementList);
													
													finalNodeElementList = new ArrayList<String>();
													finalNodeElementList.add("<span class='firsttext'>"+finalEndPoint.getId()+"</span>");
													finalNodeElementList.add("(<span class='secondtext'>"+routeNode2Id+"</span>)");
													nodePathElementsIdMap.put(finalEndPoint, finalNodeElementList);
												}
											}											
										}
									}
								}

								if(nodeSet!=null && nodeSet.getNodeList()!=null){
									currentNodeList.addAll(nodeSet.getNodeList());	
								}
							}
							relationList = null;
						}							

						routeNodeList = new ArrayList<Node>();
						if(currentNodeList!=null && !currentNodeList.isEmpty()){
							routeNodeList.addAll(currentNodeList);
						}else{
//							if(routeRelationId.contains("AI")){
//								System.out.println("$$$$$ DATA NOT FOUND BETWEEN "+routeNode1Id+" - "+routeNode2Id+" with relation "+routeRelationId+", route="+route);
//							}
							break;
						}
					}
//					System.out.println("[COMPLETED] route = "+route);
					if(routeNodeList!=null && !routeNodeList.isEmpty()){						
						List<Node> parentPoolList = getParentPoolFromParentChildMap(parentChildMap);
						NodeTree previewNodeTree = new NodeTreeImpl();
						previewNodeTree.setRootNode(true);
						List<NodeTree> dependendNodeTreeList = getNodeTreeFromParentChildMap(parentChildMap, parentPoolList);
						previewNodeTree.setDependTreeList(dependendNodeTreeList);

						List<List<Node>> allNodePathList = new ArrayList<List<Node>>();
						List<List<Node>> allNodeReversePathList = new ArrayList<List<Node>>();
						getAllPossibleNodePathList(new ArrayList<Node>(),allNodePathList,previewNodeTree);

						Map<Node,List<String>> nodePathElementsMapForReversePaths = new HashMap<Node,List<String>>();
						for(Map.Entry<Node, List<String>> e : nodePathElementsMap.entrySet()) {
							Node node = e.getKey();
							List<String> elementList = e.getValue();
							List<String> list = new ArrayList<String>();
							for(String element : elementList) {
								list.add(element);								
							}
							nodePathElementsMapForReversePaths.put(node,list);
						}
						
						Map<Node,List<String>> nodePathElementsIdMapForReversePaths = new HashMap<Node,List<String>>();
						for(Map.Entry<Node, List<String>> e : nodePathElementsIdMap.entrySet()) {
							Node node = e.getKey();
							List<String> elementList = e.getValue();
							List<String> list = new ArrayList<String>();
							for(String element : elementList) {
								list.add(element);								
							}
							nodePathElementsIdMapForReversePaths.put(node,list);
						}

						List<String> pathKeyDatasourceList = new ArrayList<String>();
						String pathKey = formatString(route, "|");
						String pathKeyDatasource = null;
						if(pathKeyDatasouceMap!=null){
							pathKeyDatasource = pathKeyDatasouceMap.get(pathKey);
							if(pathKeyDatasource!=null){
								pathKeyDatasourceList.add(pathKeyDatasource);								
							}
						}
						
						for(List<Node> path : allNodePathList) 
						{
							traverseDatasourcePaths(path, pathKeyDatasourceList, routeNodeList, nodeToNodeTypeMap, nodePathElementsMap, nodePathElementsIdMap, sourceNodeTypeNode, destinationNodeTypeNode, nodeCombinationDatasourcePathMap, nodeCombinationDatasourcePathIdsMap);
							
							List<Node> reversePath = new ArrayList<Node>();
							reversePath.addAll(path);
							Collections.reverse(reversePath);
							allNodeReversePathList.add(reversePath);
						}

						List<String> reversePathKeyDatasourceList = new ArrayList<String>();
						List<String> reverseRoute = new ArrayList<String>();
						for(int i=route.size()-1;i>=0;i--){
							reverseRoute.add(route.get(i));
						}
						String reversePathKey = formatString(reverseRoute, "|");
						String reversePathKeyDatasource = null;
						if(pathKeyDatasouceMap!=null){
							reversePathKeyDatasource = pathKeyDatasouceMap.get(reversePathKey);
							if(reversePathKeyDatasource!=null) {
								reversePathKeyDatasourceList.add(reversePathKeyDatasource);								
							}
						}
						
						for(List<Node> reversePath : allNodeReversePathList) 
						{
							Map<Node,List<String>> modifiedNodePathElementsMapForReversePaths = new HashMap<Node,List<String>>();
							for(Map.Entry<Node, List<String>> e : nodePathElementsMapForReversePaths.entrySet()) {
								Node node = e.getKey();
								List<String> elementList = e.getValue();
								List<String> list = new ArrayList<String>();
								for(String element : elementList) {
									list.add(element);								
								}
								modifiedNodePathElementsMapForReversePaths.put(node,list);
							}
							
							Map<Node,List<String>> modifiedNodePathElementsIdMapForReversePaths = new HashMap<Node,List<String>>();
							for(Map.Entry<Node, List<String>> e : nodePathElementsIdMapForReversePaths.entrySet()) {
								Node node = e.getKey();
								List<String> elementList = e.getValue();
								List<String> list = new ArrayList<String>();
								for(String element : elementList) {
									list.add(element);								
								}
								modifiedNodePathElementsIdMapForReversePaths.put(node,list);
							}
							
							for(int i=0;i<reversePath.size()-1;i++){
								Node currentNode = reversePath.get(i);
								Node nextNode = reversePath.get(i+1);
								String nextRelationName = modifiedNodePathElementsMapForReversePaths.get(nextNode).get(modifiedNodePathElementsMapForReversePaths.get(nextNode).size()-1);
								modifiedNodePathElementsMapForReversePaths.get(nextNode).remove(modifiedNodePathElementsMapForReversePaths.get(nextNode).size()-1);
								modifiedNodePathElementsMapForReversePaths.get(currentNode).add(nextRelationName);

								nextRelationName = modifiedNodePathElementsIdMapForReversePaths.get(nextNode).get(modifiedNodePathElementsIdMapForReversePaths.get(nextNode).size()-1);
								modifiedNodePathElementsIdMapForReversePaths.get(nextNode).remove(modifiedNodePathElementsIdMapForReversePaths.get(nextNode).size()-1);
								modifiedNodePathElementsIdMapForReversePaths.get(currentNode).add(nextRelationName);
							}

							traverseDatasourcePaths(reversePath, reversePathKeyDatasourceList, routeNodeList, nodeToNodeTypeMap, modifiedNodePathElementsMapForReversePaths, modifiedNodePathElementsIdMapForReversePaths, destinationNodeTypeNode, sourceNodeTypeNode, nodeCombinationDatasourcePathMap, nodeCombinationDatasourcePathIdsMap);							
						}

						for(Node node : routeNodeList) {
							finalNodeList.add(node);

							if(!reverseNodeMap.containsKey(node.getId())) {
								reverseNodeMap.put(node.getId(), new ArrayList<Node>());
							}
							reverseNodeMap.get(node.getId()).add(startNode);
						}
					}
				}
			}
		}else{
			System.out.println("NO ROUTES FOUND BETWEEN "+sourceNodeTypeNode.getName()+" and "+destinationNodeTypeNode.getName());
		}
		
		return finalNodeList;
	}

	public static void traverseDatasourcePaths(List<Node> path, List<String> pathKeyDatasourceList, List<Node> routeNodeList, Map<String,Node> nodeToNodeTypeMap, Map<Node,List<String>> nodePathElementsMap,
			Map<Node,List<String>> nodePathElementsIdMap, Node sourceNodeTypeNode, Node destinationNodeTypeNode, Map<String, List<String>> nodeCombinationDatasourcePathMap,
			Map<String, List<String>> nodeCombinationDatasourcePathIdsMap) throws Exception{
		if(containsOneOfThem(path, routeNodeList)) {
			List<StringBuffer> sbufList = new ArrayList<StringBuffer>();
			StringBuffer sbuf = new StringBuffer();
			sbufList.add(sbuf);
			List<StringBuffer> sbufIdsList = new ArrayList<StringBuffer>();
			StringBuffer sbufIds = new StringBuffer();
			sbufIdsList.add(sbufIds);
			String startNodeId = path.get(0).getId();
			String endNodeId = path.get(path.size()-1).getId();
			boolean isSkipNext = false;
			for(int i=0;i<path.size();i++) {
				Node node1 = path.get(i);

				boolean isExistingPathFound = false;
				if(i<path.size()-1){
					Node node2 = path.get(i+1);
					String nodeTypeName1 = nodeToNodeTypeMap.get(node1.getId()).getName();
					String nodeTypeName2 = nodeToNodeTypeMap.get(node2.getId()).getName();
					
					List<String> nameList = new ArrayList<String>();
					nameList.add(node1.getName());
					nameList.add(nodeTypeName1);
					nameList.add(node2.getName());
					nameList.add(nodeTypeName2);
					
					String datasourceNodeCombinationName = getPROPOSEDNodeCombinationId(nameList);
					Node datasourceNodeCombinationNode = getNodeByName(datasourceNodeCombinationName);
					if(datasourceNodeCombinationNode!=null) {
						String datasources = null;
						String datasourcesIds = null;
						
						NodeSet datasourceNodeSet = getNodeSet(datasourceNodeCombinationNode.getId(), "hasNodeTypeNodePairs", "Datasource");
						if(datasourceNodeSet!=null && datasourceNodeSet.getNodeList()!=null){
							List<Node> datasourceNodeList = datasourceNodeSet.getNodeList();
							datasources = datasourceNodeList.get(0).getName();
							datasourcesIds = datasourceNodeList.get(0).getId();
							for(int k=1;k<datasourceNodeList.size();k++){
								datasources = datasources + "," + datasourceNodeList.get(k).getName();
								datasourcesIds = datasourcesIds + "," + datasourceNodeList.get(k).getId();
							}							
						}
						
						if(i==0){
							String relationName = null;
							if(nodePathElementsMap.get(node1).size()==3){
								relationName = nodePathElementsMap.get(node1).get(nodePathElementsMap.get(node1).size()-1);
								nodePathElementsMap.get(node1).remove(2);							
							}

							if(nodePathElementsMap.get(node1).size()<=3){
								nodePathElementsMap.get(node1).add("(<span class='thirdtext'>"+datasources+"</span>)");
							}
							
							if(relationName!=null){
								nodePathElementsMap.get(node1).add(relationName);							
							}	
							
							
							relationName = null;
							if(nodePathElementsIdMap.get(node1).size()==3){
								relationName = nodePathElementsIdMap.get(node1).get(nodePathElementsIdMap.get(node1).size()-1);
								nodePathElementsIdMap.get(node1).remove(2);							
							}

							if(nodePathElementsIdMap.get(node1).size()<=3){
								nodePathElementsIdMap.get(node1).add("(<span class='thirdtext'>"+datasourcesIds+"</span>)");
							}
							
							if(relationName!=null){
								nodePathElementsIdMap.get(node1).add(relationName);							
							}	
						}

						String relationName = null;
						if(nodePathElementsMap.get(node2).size()==3){
							relationName = nodePathElementsMap.get(node2).get(nodePathElementsMap.get(node2).size()-1);
							nodePathElementsMap.get(node2).remove(2);							
						}

						if(nodePathElementsMap.get(node2).size()<=3){
							nodePathElementsMap.get(node2).add("(<span class='thirdtext'>"+datasources+"</span>)");
						}	
						
						if(relationName!=null){
							nodePathElementsMap.get(node2).add(relationName);							
						}
						
						
						relationName = null;
						if(nodePathElementsIdMap.get(node2).size()==3){
							relationName = nodePathElementsIdMap.get(node2).get(nodePathElementsIdMap.get(node2).size()-1);
							nodePathElementsIdMap.get(node2).remove(2);							
						}

						if(nodePathElementsIdMap.get(node2).size()<=3){
							nodePathElementsIdMap.get(node2).add("(<span class='thirdtext'>"+datasourcesIds+"</span>)");
						}	
						
						if(relationName!=null){
							nodePathElementsIdMap.get(node2).add(relationName);							
						}
					}
				}

				if(!isExistingPathFound){
					List<StringBuffer> modifiedSbufList = new ArrayList<StringBuffer>();
					List<StringBuffer> modifiedSbufIdsList = new ArrayList<StringBuffer>();
					boolean isChangeSkipNext = false;
					for(StringBuffer sb : sbufList){
						StringBuffer modifiedSbuf = new StringBuffer();
						modifiedSbuf.append(sb);
						if(!isSkipNext){
							for(String element : nodePathElementsMap.get(node1)){
								modifiedSbuf.append(element);									
							}							
						}else{
							if(nodePathElementsMap.get(node1).get(nodePathElementsMap.get(node1).size()-1).startsWith("<br>-") && nodePathElementsMap.get(node1).get(nodePathElementsMap.get(node1).size()-1).endsWith("-<br>")){
								String relationName = nodePathElementsMap.get(node1).get(nodePathElementsMap.get(node1).size()-1);
								modifiedSbuf.append(relationName);										
							}
							isChangeSkipNext = true;
						}
						modifiedSbufList.add(modifiedSbuf);
					}
					
					for(StringBuffer sb : sbufIdsList){
						StringBuffer modifiedSbuf = new StringBuffer();
						modifiedSbuf.append(sb);
						if(!isSkipNext){
							for(String element : nodePathElementsIdMap.get(node1)){
								modifiedSbuf.append(element);									
							}							
						}else{
							if(nodePathElementsIdMap.get(node1).get(nodePathElementsIdMap.get(node1).size()-1).startsWith("<br>-") && nodePathElementsIdMap.get(node1).get(nodePathElementsIdMap.get(node1).size()-1).endsWith("-<br>")){
								String relationName = nodePathElementsIdMap.get(node1).get(nodePathElementsIdMap.get(node1).size()-1);
								modifiedSbuf.append(relationName);										
							}
							isChangeSkipNext = true;
						}
						modifiedSbufIdsList.add(modifiedSbuf);
					}

					if(isChangeSkipNext){
						isSkipNext = false;
					}
					
					sbufList.clear();
					sbufList.addAll(modifiedSbufList);						

					sbufIdsList.clear();
					sbufIdsList.addAll(modifiedSbufIdsList);						
				}
			}

//			logger.info(sbufList.toString());
			String nodeCombinationName = startNodeId + "|" + sourceNodeTypeNode.getId() + "|" + endNodeId + "|" + destinationNodeTypeNode.getId();
			
			if(!nodeCombinationDatasourcePathMap.containsKey(nodeCombinationName)){
				nodeCombinationDatasourcePathMap.put(nodeCombinationName, new ArrayList<String>());
			}

			if(!nodeCombinationDatasourcePathIdsMap.containsKey(nodeCombinationName)){
				nodeCombinationDatasourcePathIdsMap.put(nodeCombinationName, new ArrayList<String>());
			}

			for(StringBuffer sb : sbufList){
				nodeCombinationDatasourcePathMap.get(nodeCombinationName).add(sb.toString());
			}
			for(StringBuffer sb : sbufIdsList){
				nodeCombinationDatasourcePathIdsMap.get(nodeCombinationName).add(sb.toString());
			}
		}
	}

	public static boolean containsOneOfThem(List<Node> sourceList, List<Node> targetList) {
		// Both are null
		if(sourceList == null && targetList == null) {
			return true;
		}
		
		// Either of them is null
		if(sourceList == null || targetList == null) {
			return false;
		}
		
		// Both the list are not null
		for(Node element : targetList)
		{
			if(sourceList.contains((Node)element)) {
				return true;
			}
		}
		
		return false;
	}

	public static Map<String,String> getNRNFromFile() {
		Map<String,String> nrnRelationMap = new HashMap<String,String>();
		File pathFolder = new File("D:\\uploadedFiles\\NRNRelation");
		try {
			if(pathFolder.listFiles()!=null && pathFolder.listFiles().length>0){
				File file = getLatestFileFromDirectory(pathFolder);
				if(file!=null){
					List<FileDataSet> fdsList = getFileDataSetSheetWiseList(file.getAbsolutePath(), "MSEXCEL", false, 0);
					if(fdsList!=null && !fdsList.isEmpty()){
						for(FileDataSet fds : fdsList){
							List<String> headerList = fds.getHeaderNameList();
							if(headerList!=null && headerList.contains("Start Node Type") && headerList.contains("End Node Type") && headerList.contains("Relation Name")){
								List<Map<String,String>> sheetDataMap = fds.getDataMapList();
								for(Map<String,String> rowMap : sheetDataMap){
									String startNodeName = rowMap.get("Start Node Type");
									String endNodeName = rowMap.get("End Node Type");
									String relationNameFromFile = rowMap.get("Relation Name");
									nrnRelationMap.put(startNodeName.trim().toLowerCase()+"|"+endNodeName.trim().toLowerCase(), relationNameFromFile.trim());
									nrnRelationMap.put(endNodeName.trim().toLowerCase()+"|"+startNodeName.trim().toLowerCase(), relationNameFromFile.trim());
								}
							}else {
								throw new Exception();
							}
						}
					}
				}
			}
		}catch(Exception e) {
			e.printStackTrace();
			System.out.println(e);
		}
		return nrnRelationMap;
	}
	
	public static String getHash(String plainText) 
	{
		MessageDigest md = null;
		try {
			plainText = plainText.toLowerCase();
			md = MessageDigest.getInstance("MD5"); 
			md.update(plainText.getBytes("UTF-8")); 
			byte mdbytes[] = md.digest(); 
			//convert the byte to hex format method 1
	        StringBuffer sb = new StringBuffer();
	        for (int i = 0; i < mdbytes.length; i++) {
	          sb.append(Integer.toString((mdbytes[i] & 0xff) + 0x100, 16).substring(1));
	        }
	        return sb.toString();
		} catch(NoSuchAlgorithmException e) {
			System.out.println(e.getMessage());
			return null;
		} catch(UnsupportedEncodingException e) {
			System.out.println(e.getMessage());
			return null;
		}
	}

	public static String getProposedAIRelationName(String name1, String name2) {
		if((name1 == null || name1.isEmpty()) && (name2 == null || name2.isEmpty())) {
			return "";
		}
		List<String> list = new ArrayList<String>();
		list.add(name1);
		list.add(name2);
		Collections.sort(list);
		
		String relationName = "AI_Has"+list.get(0)+"-"+list.get(1);
		return relationName;
	}

	public static List<String> parseStrings(String formattedString, String delimiter) {
		if(formattedString == null || delimiter == null) {
			return null;			
		}
		StringTokenizer stringTokenizer = new StringTokenizer(formattedString, delimiter);
		List<String> stringList = new ArrayList<String>();
		
        while(stringTokenizer.hasMoreTokens()) {
        	stringList.add(stringTokenizer.nextToken());
        }
        return stringList;
	}

	public static File getLatestFileFromDirectory(File directory){
		if(directory!=null){
			List<File> matchingFileList = new ArrayList<File>();
	        File[] directoryFileList = directory.listFiles();
	        if(directoryFileList!=null && directoryFileList.length>0){
		        for (File directoryFile : directoryFileList){
	            	matchingFileList.add(directoryFile);
		        }
	        }
	        
	        if(!matchingFileList.isEmpty()){
	        	File latestFile = matchingFileList.get(0);
	        	Date latestFileModificationDate = new Date(matchingFileList.get(0).lastModified());
	        	for(int i=0;i<matchingFileList.size();i++){
	        		Date fileModificationDate = new Date(matchingFileList.get(i).lastModified());
	        		if(fileModificationDate.compareTo(latestFileModificationDate) > 0){
	        			latestFileModificationDate = fileModificationDate;
	        			latestFile = matchingFileList.get(i);
	        		}
	        	}
	       		return latestFile;
	        }
		}
		return null;
	}
	
	public static List<FileDataSet> getFileDataSetSheetWiseList(String filePath, String type, boolean isConcatenateIndex, int startRowNo) throws Exception{
		List<FileDataSet> fdsList = new ArrayList<FileDataSet>();
		List<String> sheetNameList = new ArrayList<String>();

		FileReaderHelper fileReader = null;
		fileReader = new FileReaderMemoryOptimized(filePath,isConcatenateIndex,startRowNo);
		
		sheetNameList = fileReader.getSheetList();
		for(String sheetName : sheetNameList){
			fdsList.add(getFileDataSet(filePath, type, isConcatenateIndex, startRowNo, sheetName, sheetNameList.indexOf(sheetName), null));
		}
		return fdsList;
	}

	public static FileDataSet getFileDataSet(String filePath, String type, boolean isConcatenateIndex, int startRowNo, String sheetName, int sheetIndex, List<Integer> headerRowNumberList) throws Exception{
		List<String> headerNameList = new ArrayList<String>();
		Map<String,String> firstRowMap = new HashMap<String,String>();
		List<Map<String,String>> dataMapList = new ArrayList<Map<String,String>>();
		FileDataSet fds = null;

		FileReaderHelper fileReader = null;
		fds = new FileDataSet(filePath,sheetName);

		fileReader = getExcelOrCSVFileReader(filePath,isConcatenateIndex,startRowNo,sheetName,sheetIndex,headerRowNumberList);
		headerNameList = fileReader.readHeaderRow();
		
		fileReader = getExcelOrCSVFileReader(filePath,isConcatenateIndex,startRowNo,sheetName,sheetIndex,headerRowNumberList);
		firstRowMap = fileReader.readFirstRow();

		fileReader = getExcelOrCSVFileReader(filePath,isConcatenateIndex,startRowNo,sheetName,sheetIndex,headerRowNumberList);
		Map<String,List<Map<String,String>>> sheetDataMap = fileReader.readAll();
		dataMapList.addAll(sheetDataMap.get(sheetName));			

		fds.setHeaderNameList(headerNameList);
		fds.setFirstRowMap(firstRowMap);
		fds.setDataMapList(dataMapList);
		return fds;
	}
	
	private static FileReaderHelper getExcelOrCSVFileReader(String filePath, boolean isConcatenateIndex, int startRowNo, String sheetName, int sheetIndex, List<Integer> headerRowNumberList){
		FileReaderHelper fileReader = null;
		if(headerRowNumberList==null){
			if(sheetName!=null && !sheetName.equals("")){
				fileReader = new FileReaderMemoryOptimized(filePath,isConcatenateIndex,startRowNo,sheetName);
			}else{
				fileReader = new FileReaderMemoryOptimized(filePath,isConcatenateIndex,startRowNo,sheetIndex);
			}				
		}else{
			if(sheetName!=null && !sheetName.equals("")){
				fileReader = new FileReaderMemoryOptimized(filePath,isConcatenateIndex,headerRowNumberList);
				fileReader.setSheetName(sheetName);
			}else{
				fileReader = new FileReaderMemoryOptimized(filePath,isConcatenateIndex,headerRowNumberList);
				fileReader.setSheetIndex(sheetIndex);
			}
		}
		return fileReader;
	}
	
	public static List<Node> getParentPoolFromParentChildMap(Map<Node, Set<Node>> contextPoolContextTagMap) {
		boolean isParentPool;
		List<Node> parentList = new ArrayList<Node>(); 
		for(Node parent : contextPoolContextTagMap.keySet()){
			isParentPool = true;
			for(Set<Node> children : contextPoolContextTagMap.values()){
				if(children.contains(parent)){
					isParentPool  = false;					
					break;
				}
			}
			if(isParentPool) parentList.add(parent);
		}

		return parentList;
	}

	public static List<NodeTree> getNodeTreeFromParentChildMap(Map<Node,Set<Node>> parentChildMap, List<Node> parentPoolList){
		
		List<NodeTree> nodeTreeList = new ArrayList<NodeTree>(); 
		
		for(Node parentPool : parentPoolList){
			NodeTree nodeTree = new NodeTreeImpl();
			List<NodeTree> chidNodeTreeList = new ArrayList<NodeTree>() ;
			nodeTree.setCurrentNode(parentPool);
			
			if(parentChildMap.get(parentPool)!=null){
				for(Node tag : parentChildMap.get(parentPool)){
					List<Node> tagList = new ArrayList<Node>();
					tagList.add(tag);
					chidNodeTreeList.addAll(getNodeTreeFromParentChildMap(parentChildMap, tagList));
				}
			}
			nodeTree.setDependTreeList(chidNodeTreeList.isEmpty() ? null : chidNodeTreeList);
			nodeTreeList.add(nodeTree);
		}
		
		return nodeTreeList;
	}

	public static void getAllPossibleNodePathList(List<Node> pathNodeList,List<List<Node>> allPathNodeList, NodeTree nodeTree) {
		if(nodeTree.getCurrentNode() != null){
			pathNodeList.add(nodeTree.getCurrentNode());
		}
		if(nodeTree.getDependTreeList() != null) {
			for(NodeTree childNodeTree : nodeTree.getDependTreeList()) {
				getAllPossibleNodePathList(pathNodeList,allPathNodeList, childNodeTree);
			}
		}else{
			List<Node> newPathNodeList = new ArrayList<Node>(pathNodeList);
			allPathNodeList.add(newPathNodeList);
		}
		if(nodeTree.getCurrentNode() != null){
			pathNodeList.remove(nodeTree.getCurrentNode());
		}
	}

	public static String getPROPOSEDNodeCombinationId(List<String> nodeIdList) {
		if(nodeIdList == null || nodeIdList.isEmpty()) {
			return "";
		}
		 List<String> concurrentArrayList = new CopyOnWriteArrayList<String>(nodeIdList);
		if(concurrentArrayList.size() == 1) {
//			return nodeIdList.get(0)+"|";
			return concurrentArrayList.get(0); // Changing in redesign because if we append | after this then and Pune and Pune| will become two different nodes..which should not happen
		}
		String nodeCombinationId = null;
		Collections.sort(concurrentArrayList);
		nodeCombinationId = formatString(concurrentArrayList, "|");
		if(concurrentArrayList.size() > 100) {
			return "##NC-HASH-CASE##" + getHash(nodeCombinationId);
		} else {
			return nodeCombinationId;
		}
	}

	public static String formatString(List<String> strList, String delimiter) {
		if(strList == null || strList.size() == 0) {
			return null;
		}
		
		StringBuffer formattedStr = new StringBuffer();
		for(int i=0; i<strList.size(); i++) {
			if(i != 0) {
				formattedStr.append(delimiter);
			}
			formattedStr.append(strList.get(i));
		}
		return formattedStr.toString();
	}

	public static void main(String[] args) {
		try {	
			Map<String,String> nrnRelationMap = getNRNFromFile();
			createPaths(nrnRelationMap);
		}catch(Exception e) {
			e.printStackTrace();
			System.out.println("Error occurred while populating nodes");
		}
	}
}

class TraverseAPICallableNew implements Callable<TraverseAPIDataNew>, Serializable {
	private static final long serialVersionUID = 1L;
	Node node;
	Node nodeTypeNode1;
	Node nodeTypeNode2;
	static List<List<String>> allRoutesBetweenNodes;
	static Map<String,String> nodeTypeIdNameMap;
	static Map<String, String> pathKeyDatasouceMap;
	static Map<String, String> masterNodeMap;
	static Map<String, NodeSet> masterNodeNodeSetMap;
	String relationName;
	String directRelationName;
	static Map<String,List<String>> nodeCombinationDatasourcePathMap;
	static Map<String,List<String>> nodeCombinationDatasourcePathIdMap;
	
	public TraverseAPICallableNew(Node node, Node nodeTypeNode1, Node nodeTypeNode2, List<List<String>> allRoutesBetweenNodes, Map<String,String> nodeTypeIdNameMap, Map<String, String> pathKeyDatasouceMap,String relationName,
			String directRelationName, Map<String, List<String>> nodeCombinationDatasourcePathMap, Map<String, List<String>> nodeCombinationDatasourcePathIdMap)
	{
		this.node = node;
		this.nodeTypeNode1 = nodeTypeNode1;
		this.nodeTypeNode2 = nodeTypeNode2;
		TraverseAPICallableNew.allRoutesBetweenNodes = allRoutesBetweenNodes;
		TraverseAPICallableNew.nodeTypeIdNameMap = nodeTypeIdNameMap;
		TraverseAPICallableNew.pathKeyDatasouceMap = pathKeyDatasouceMap;
		this.relationName = relationName;
		this.directRelationName = directRelationName;
		TraverseAPICallableNew.nodeCombinationDatasourcePathMap = nodeCombinationDatasourcePathMap;
		TraverseAPICallableNew.nodeCombinationDatasourcePathIdMap = nodeCombinationDatasourcePathIdMap;
	}

	public TraverseAPIDataNew call() throws Exception {
		Map<String,List<Node>> reverseNodeMap = null;
		Map<String,List<Node>> forwardMap = null;
		Map<String,List<Node>> reverseMap = null;
		Map<String,List<Node>> forwardDirectRelationMap = null;
		Map<String,List<Node>> reverseDirectRelationMap = null;
		reverseNodeMap = new HashMap<String,List<Node>>();
		forwardMap = new HashMap<String,List<Node>>();
		reverseMap = new HashMap<String,List<Node>>();
		forwardDirectRelationMap = new HashMap<String,List<Node>>();
		reverseDirectRelationMap = new HashMap<String,List<Node>>();			
		
		List<Node> finalNodeList =  new ArrayList<Node>();
		List<Node> outputNodeList = PopulateAIRelation.traversal_API(this.node, nodeTypeNode1, nodeTypeNode2, allRoutesBetweenNodes, nodeTypeIdNameMap,reverseNodeMap,pathKeyDatasouceMap,masterNodeMap,masterNodeNodeSetMap,nodeCombinationDatasourcePathMap,nodeCombinationDatasourcePathIdMap);
		if(outputNodeList!=null){
			finalNodeList = outputNodeList;
		}

//		System.out.println("[COMPLETED] traversing for node = "+nodeNameNode.getName());

		if(finalNodeList!=null){
			reverseMap = reverseNodeMap;
		}
		
		forwardMap.put(this.node.getId(), finalNodeList);
		
		if(directRelationName!=null){
			NodeSet nodeSet = PopulateAIRelation.getNodeSet(this.node.getId(),directRelationName.trim(),nodeTypeNode2.getName());
			List<Node> modifiedNodeList = new ArrayList<Node>();
			if(nodeSet!=null && nodeSet.getNodeList()!=null){
				for(Node node : nodeSet.getNodeList()){
					String nodeId = node.getId();
					
					modifiedNodeList.add(node);
					
					if(!reverseDirectRelationMap.containsKey(nodeId)){
						reverseDirectRelationMap.put(nodeId, new ArrayList<Node>());
					}
					reverseDirectRelationMap.get(nodeId).add(this.node);									

				}
			}
			nodeSet = null;
			forwardDirectRelationMap.put(this.node.getId(), modifiedNodeList);
		}
		reverseNodeMap = null;
		TraverseAPIDataNew data = new TraverseAPIDataNew(forwardMap,reverseMap,forwardDirectRelationMap,reverseDirectRelationMap);

		return data;
	}
}

class TraverseAPIDataNew
{
	Map<String,List<Node>> forwardMap;
	Map<String,List<Node>> reverseMap;
	Map<String,List<Node>> forwardDirectRelationMap;
	Map<String,List<Node>> reverseDirectRelationMap;

	public TraverseAPIDataNew()
	{
		
	}

	public TraverseAPIDataNew(Map<String,List<Node>> forwardMap, Map<String,List<Node>> reverseMap, Map<String,List<Node>> forwardDirectRelationMap,
			Map<String,List<Node>> reverseDirectRelationMap)
	{
		super();
		this.forwardMap = forwardMap;
		this.reverseMap = reverseMap;
		this.forwardDirectRelationMap = forwardDirectRelationMap;
		this.reverseDirectRelationMap = reverseDirectRelationMap;
	}

	public TraverseAPIDataNew getData() {
		return this;
	}

	public void putData(Map<String,List<Node>> forwardMap, Map<String,List<Node>> reverseMap, Map<String,List<Node>> forwardDirectRelationMap,
			Map<String,List<Node>> reverseDirectRelationMap) {
		this.forwardMap = forwardMap;
		this.reverseMap = reverseMap;
		this.forwardDirectRelationMap = forwardDirectRelationMap;
		this.reverseDirectRelationMap = reverseDirectRelationMap;
	}
}

interface Node extends Comparable<Node>, Serializable {
	public String getId();
	public void setId(String id);
	
	public String getName();
	public void setName(String name);
	
	public String getProperty(String key);
	public void setProperty(String key, String value);
	public Set<String> getPropertyKeySet();
	public void removeProperty(String key);
	public Map<String, String> getPropertyValueMap();
	public void setPropertyValueMap(Map<String, String> propertyValueMap);
	
	public List<Relation> getRelationList();
	public List<Relation> getRelationList(Direction direction);
	public void setRelationList(List<Relation> relationList);
	
	public int compareTo(Node node);
	public Node clone();
	
	public String toJSONString() throws JSONException;
	public String toLightJSONString() throws JSONException;
	public JSONObject toJSONObject() throws JSONException;
}

class NodeImpl implements Node, Serializable, Comparable<Node> {
	private static final long serialVersionUID = 1L;
	
	private String id;
	private Map<String, String> propertyValueMap;
	private List<Relation> relationList;
	
	public NodeImpl() {
		this.propertyValueMap = new LinkedHashMap<String, String>();
		this.relationList = new ArrayList<Relation>();
	}

	public NodeImpl(String id, String name) {
		this.propertyValueMap = new LinkedHashMap<String, String>();
		this.relationList = new ArrayList<Relation>();
		this.id = id;
		setName(name);
	}

	public NodeImpl(String jsonString) throws JSONException {
		this.propertyValueMap = new LinkedHashMap<String, String>();
		this.relationList = new ArrayList<Relation>();
		
		JSONObject newJSONNode = new JSONObject(jsonString);
		
		setId(newJSONNode.optString("id"));
		if(newJSONNode.optJSONArray("properties") != null){
			JSONArray propertyArray = newJSONNode.optJSONArray("properties");
			if(propertyArray != null) {
				for(int i=0; i<propertyArray.length(); i++) {
					JSONObject property = propertyArray.getJSONObject(i);
					String name = property.optString("name");
					String value = property.optString("value");
					propertyValueMap.put(name, value);
				}
			}
		}
	}
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return propertyValueMap.get("name");
	}

	public void setName(String name) {
		propertyValueMap.put("name", name);
	}
	
	public String getProperty(String key) {
		if(key == null) {
			return null;
		}
		return propertyValueMap.get(key);
	}

	public void setProperty(String key, String value) {
		if(key == null) {
			throw new IllegalArgumentException("Invalid key=" + key);
		}
		propertyValueMap.put(key, value);
	}

	public Set<String> getPropertyKeySet() {
		return propertyValueMap.keySet();
	}

	public void removeProperty(String key) {
		propertyValueMap.remove(key);
	}
	
	public List<Relation> getRelationList() {
		return this.relationList;
	}

	public List<Relation> getRelationList(Direction direction) {
		List<Relation> relations = new ArrayList<Relation>();
		for(Relation relation : relationList) {
			if(Direction.INCOMING.compareTo(direction) == 0) {
				if(relation.getEndNode().getId().equals(id)) {
					relations.add(relation);
				}
			} else if(Direction.OUTGOING.compareTo(direction) == 0) {
				if(relation.getStartNode().getId().equals(id)) {
					relations.add(relation);
				}
			} else {
				relations.add(relation);
			}
		}
		return relations;
	}

	public void setRelationList(List<Relation> relationList) {
		if(relationList == null) {
			return;
		}
		this.relationList.addAll(relationList);
	}

	public Map<String, String> getPropertyValueMap() {
		return propertyValueMap;
	}

	public void setPropertyValueMap(Map<String, String> propertyValueMap) {
		this.propertyValueMap = propertyValueMap;
	}

	public boolean equals(Object obj)
	{
		if(this == obj) {
			return true;
		}
		if((obj == null) || (obj.getClass() != this.getClass())) {
			return false;
		}
		// Object must be CalendarTimeUnit at this point
		Node anotherNode = (Node)obj;
		if(this.id == null || anotherNode.getId() == null) {
			return false;
		}
		
		if(this.getName() == null || anotherNode.getName() == null) {
			return false;
		}
		
		return (this.getName().equals(anotherNode.getName()) && 
				this.id.equals(anotherNode.getId()));
	}

	public int hashCode()
	{
		int hash = 7;
		hash = 31 * hash + this.id.hashCode();
		hash = 31 * hash + this.getName().hashCode();
		return hash;
	}
	
	public String toString() {
		try {
			return toJSONString();
		} catch (JSONException e) {
			return "";
		}
	}
	
	public Node clone() {
		return (Node)CloneUtil.clone(this);
	}
	
	public String toJSONString() throws JSONException {
		return toJSONObject().toString();
	}
	
	public int compareTo(Node node)
	{
	    final int BEFORE = -1;
	    final int EQUAL = 0;
	    final int AFTER = 1;
	    if(this.getName().compareTo(node.getName()) == 0) {
	    	return EQUAL;
	    }
	    if(this.getName().compareTo(node.getName()) > 0) {
	    	return AFTER;
	    }
	    if(this.getName().compareTo(node.getName()) < 0) {
	    	return BEFORE;
	    }
	   return EQUAL;
	}
	
	public JSONObject toJSONObject() throws JSONException {
		JSONObject entity = new JSONObject();
		entity.put("id", id);
		entity.put("name", getName());
		JSONArray propertyArray = new JSONArray();
		for(String key : propertyValueMap.keySet()) {
			JSONObject property = new JSONObject();
			property.put("name", key);
			property.put("value", propertyValueMap.get(key));
			propertyArray.put(property);
		}
		entity.put("properties", propertyArray);

		JSONArray incomingNodeArray = new JSONArray();
		for(Relation relation : getRelationList(Direction.INCOMING)) {
			incomingNodeArray.put(relation.toJSONObject());
		}
		entity.put("incoming", incomingNodeArray);

		JSONArray outgoingNodeArray = new JSONArray();
		for(Relation relation : getRelationList(Direction.OUTGOING)) {
			outgoingNodeArray.put(relation.toJSONObject());
		}
		entity.put("outgoing", outgoingNodeArray);
		
		return entity;
	}
	
	public String toLightJSONString() throws JSONException {
		JSONObject entity = new JSONObject();
		entity.put("id", id);
		entity.put("name", getName());
		
		JSONArray propertyArray = new JSONArray();
		JSONObject property = new JSONObject();
		property.put("name", "name");
		property.put("value", getName());
		propertyArray.put(property);
		entity.put("properties", propertyArray);

		return entity.toString();
	}
	
	
	public static <T extends Node> JSONArray toJSONArray(List<? extends Node> tList) throws JSONException {
		JSONArray jsonArray = new JSONArray();
		for(Node entity : tList) {
			jsonArray.put(entity.toJSONObject());
		}
		return jsonArray;
	}

	public static <T extends Node> String toJSONArrayString(List<? extends Node> tList) throws JSONException {
		return toJSONArray(tList).toString();
	}
}

class JSONArray {


    /**
     * The arrayList where the JSONArray's properties are kept.
     */
    private ArrayList myArrayList;


    /**
     * Construct an empty JSONArray.
     */
    public JSONArray() {
        this.myArrayList = new ArrayList();
    }

    /**
     * Construct a JSONArray from a JSONTokener.
     * @param x A JSONTokener
     * @throws JSONException If there is a syntax error.
     */
    public JSONArray(JSONTokener x) throws JSONException {
        this();
        if (x.nextClean() != '[') {
            throw x.syntaxError("A JSONArray text must start with '['");
        }
        if (x.nextClean() != ']') {
	        x.back();
	        for (;;) {
	            if (x.nextClean() == ',') {
	                x.back();
	                this.myArrayList.add(JSONObject.NULL);
	            } else {
	                x.back();
	                this.myArrayList.add(x.nextValue());
	            }
	            switch (x.nextClean()) {
	            case ';':
	            case ',':
	                if (x.nextClean() == ']') {
	                    return;
	                }
	                x.back();
	                break;
	            case ']':
	            	return;
	            default:
	                throw x.syntaxError("Expected a ',' or ']'");
	            }
	        }
        }
    }


    /**
     * Construct a JSONArray from a source JSON text.
     * @param source     A string that begins with
     * <code>[</code>&nbsp;<small>(left bracket)</small>
     *  and ends with <code>]</code>&nbsp;<small>(right bracket)</small>.
     *  @throws JSONException If there is a syntax error.
     */
    public JSONArray(String source) throws JSONException {
        this(new JSONTokener(source));
    }


    /**
     * Construct a JSONArray from a Collection.
     * @param collection     A Collection.
     */
    public JSONArray(Collection collection) {
		this.myArrayList = new ArrayList();
		if (collection != null) {
			Iterator iter = collection.iterator();
			while (iter.hasNext()) {
                this.myArrayList.add(JSONObject.wrap(iter.next()));  
			}
		}
    }

    
    /**
     * Construct a JSONArray from an array
     * @throws JSONException If not an array.
     */
    public JSONArray(Object array) throws JSONException {
        this();
        if (array.getClass().isArray()) {
            int length = Array.getLength(array);
            for (int i = 0; i < length; i += 1) {
                this.put(JSONObject.wrap(Array.get(array, i)));
            }
        } else {
            throw new JSONException(
"JSONArray initial value should be a string or collection or array.");
        }
    }
    
    
    /**
     * Get the object value associated with an index.
     * @param index
     *  The index must be between 0 and length() - 1.
     * @return An object value.
     * @throws JSONException If there is no value for the index.
     */
    public Object get(int index) throws JSONException {
        Object object = opt(index);
        if (object == null) {
            throw new JSONException("JSONArray[" + index + "] not found.");
        }
        return object;
    }


    /**
     * Get the boolean value associated with an index.
     * The string values "true" and "false" are converted to boolean.
     *
     * @param index The index must be between 0 and length() - 1.
     * @return      The truth.
     * @throws JSONException If there is no value for the index or if the
     *  value is not convertible to boolean.
     */
    public boolean getBoolean(int index) throws JSONException {
        Object object = get(index);
        if (object.equals(Boolean.FALSE) ||
                (object instanceof String &&
                ((String)object).equalsIgnoreCase("false"))) {
            return false;
        } else if (object.equals(Boolean.TRUE) ||
                (object instanceof String &&
                ((String)object).equalsIgnoreCase("true"))) {
            return true;
        }
        throw new JSONException("JSONArray[" + index + "] is not a boolean.");
    }


    /**
     * Get the double value associated with an index.
     *
     * @param index The index must be between 0 and length() - 1.
     * @return      The value.
     * @throws   JSONException If the key is not found or if the value cannot
     *  be converted to a number.
     */
    public double getDouble(int index) throws JSONException {
        Object object = get(index);
        try {
            return object instanceof Number ?
                ((Number)object).doubleValue() :
                Double.parseDouble((String)object);
        } catch (Exception e) {
            throw new JSONException("JSONArray[" + index +
                "] is not a number.");
        }
    }


    /**
     * Get the int value associated with an index.
     *
     * @param index The index must be between 0 and length() - 1.
     * @return      The value.
     * @throws   JSONException If the key is not found or if the value is not a number.
     */
    public int getInt(int index) throws JSONException {
        Object object = get(index);
        try {
            return object instanceof Number ?
                ((Number)object).intValue() :
                Integer.parseInt((String)object);
        } catch (Exception e) {
            throw new JSONException("JSONArray[" + index +
                "] is not a number.");
        }
    }


    /**
     * Get the JSONArray associated with an index.
     * @param index The index must be between 0 and length() - 1.
     * @return      A JSONArray value.
     * @throws JSONException If there is no value for the index. or if the
     * value is not a JSONArray
     */
    public JSONArray getJSONArray(int index) throws JSONException {
        Object object = get(index);
        if (object instanceof JSONArray) {
            return (JSONArray)object;
        }
        throw new JSONException("JSONArray[" + index +
                "] is not a JSONArray.");
    }


    /**
     * Get the JSONObject associated with an index.
     * @param index subscript
     * @return      A JSONObject value.
     * @throws JSONException If there is no value for the index or if the
     * value is not a JSONObject
     */
    public JSONObject getJSONObject(int index) throws JSONException {
        Object object = get(index);
        if (object instanceof JSONObject) {
            return (JSONObject)object;
        }
        throw new JSONException("JSONArray[" + index +
            "] is not a JSONObject.");
    }


    /**
     * Get the long value associated with an index.
     *
     * @param index The index must be between 0 and length() - 1.
     * @return      The value.
     * @throws   JSONException If the key is not found or if the value cannot
     *  be converted to a number.
     */
    public long getLong(int index) throws JSONException {
        Object object = get(index);
        try {
            return object instanceof Number ?
                ((Number)object).longValue() :
                Long.parseLong((String)object);
        } catch (Exception e) {
            throw new JSONException("JSONArray[" + index +
                "] is not a number.");
        }
    }


    /**
     * Get the string associated with an index.
     * @param index The index must be between 0 and length() - 1.
     * @return      A string value.
     * @throws JSONException If there is no string value for the index.
     */
    public String getString(int index) throws JSONException {
        Object object = get(index);
        if (object instanceof String) {
            return (String)object;
        }
        throw new JSONException("JSONArray[" + index + "] not a string.");
    }


    /**
     * Determine if the value is null.
     * @param index The index must be between 0 and length() - 1.
     * @return true if the value at the index is null, or if there is no value.
     */
    public boolean isNull(int index) {
        return JSONObject.NULL.equals(opt(index));
    }


    /**
     * Make a string from the contents of this JSONArray. The
     * <code>separator</code> string is inserted between each element.
     * Warning: This method assumes that the data structure is acyclical.
     * @param separator A string that will be inserted between the elements.
     * @return a string.
     * @throws JSONException If the array contains an invalid number.
     */
    public String join(String separator) throws JSONException {
        int len = length();
        StringBuffer sb = new StringBuffer();

        for (int i = 0; i < len; i += 1) {
            if (i > 0) {
                sb.append(separator);
            }
            sb.append(JSONObject.valueToString(this.myArrayList.get(i)));
        }
        return sb.toString();
    }


    /**
     * Get the number of elements in the JSONArray, included nulls.
     *
     * @return The length (or size).
     */
    public int length() {
        return this.myArrayList.size();
    }


    /**
     * Get the optional object value associated with an index.
     * @param index The index must be between 0 and length() - 1.
     * @return      An object value, or null if there is no
     *              object at that index.
     */
    public Object opt(int index) {
        return (index < 0 || index >= length()) ?
            null : this.myArrayList.get(index);
    }


    /**
     * Get the optional boolean value associated with an index.
     * It returns false if there is no value at that index,
     * or if the value is not Boolean.TRUE or the String "true".
     *
     * @param index The index must be between 0 and length() - 1.
     * @return      The truth.
     */
    public boolean optBoolean(int index)  {
        return optBoolean(index, false);
    }


    /**
     * Get the optional boolean value associated with an index.
     * It returns the defaultValue if there is no value at that index or if
     * it is not a Boolean or the String "true" or "false" (case insensitive).
     *
     * @param index The index must be between 0 and length() - 1.
     * @param defaultValue     A boolean default.
     * @return      The truth.
     */
    public boolean optBoolean(int index, boolean defaultValue)  {
        try {
            return getBoolean(index);
        } catch (Exception e) {
            return defaultValue;
        }
    }


    /**
     * Get the optional double value associated with an index.
     * NaN is returned if there is no value for the index,
     * or if the value is not a number and cannot be converted to a number.
     *
     * @param index The index must be between 0 and length() - 1.
     * @return      The value.
     */
    public double optDouble(int index) {
        return optDouble(index, Double.NaN);
    }


    /**
     * Get the optional double value associated with an index.
     * The defaultValue is returned if there is no value for the index,
     * or if the value is not a number and cannot be converted to a number.
     *
     * @param index subscript
     * @param defaultValue     The default value.
     * @return      The value.
     */
    public double optDouble(int index, double defaultValue) {
        try {
            return getDouble(index);
        } catch (Exception e) {
            return defaultValue;
        }
    }


    /**
     * Get the optional int value associated with an index.
     * Zero is returned if there is no value for the index,
     * or if the value is not a number and cannot be converted to a number.
     *
     * @param index The index must be between 0 and length() - 1.
     * @return      The value.
     */
    public int optInt(int index) {
        return optInt(index, 0);
    }


    /**
     * Get the optional int value associated with an index.
     * The defaultValue is returned if there is no value for the index,
     * or if the value is not a number and cannot be converted to a number.
     * @param index The index must be between 0 and length() - 1.
     * @param defaultValue     The default value.
     * @return      The value.
     */
    public int optInt(int index, int defaultValue) {
        try {
            return getInt(index);
        } catch (Exception e) {
            return defaultValue;
        }
    }


    /**
     * Get the optional JSONArray associated with an index.
     * @param index subscript
     * @return      A JSONArray value, or null if the index has no value,
     * or if the value is not a JSONArray.
     */
    public JSONArray optJSONArray(int index) {
        Object o = opt(index);
        return o instanceof JSONArray ? (JSONArray)o : null;
    }


    /**
     * Get the optional JSONObject associated with an index.
     * Null is returned if the key is not found, or null if the index has
     * no value, or if the value is not a JSONObject.
     *
     * @param index The index must be between 0 and length() - 1.
     * @return      A JSONObject value.
     */
    public JSONObject optJSONObject(int index) {
        Object o = opt(index);
        return o instanceof JSONObject ? (JSONObject)o : null;
    }


    /**
     * Get the optional long value associated with an index.
     * Zero is returned if there is no value for the index,
     * or if the value is not a number and cannot be converted to a number.
     *
     * @param index The index must be between 0 and length() - 1.
     * @return      The value.
     */
    public long optLong(int index) {
        return optLong(index, 0);
    }


    /**
     * Get the optional long value associated with an index.
     * The defaultValue is returned if there is no value for the index,
     * or if the value is not a number and cannot be converted to a number.
     * @param index The index must be between 0 and length() - 1.
     * @param defaultValue     The default value.
     * @return      The value.
     */
    public long optLong(int index, long defaultValue) {
        try {
            return getLong(index);
        } catch (Exception e) {
            return defaultValue;
        }
    }


    /**
     * Get the optional string value associated with an index. It returns an
     * empty string if there is no value at that index. If the value
     * is not a string and is not null, then it is coverted to a string.
     *
     * @param index The index must be between 0 and length() - 1.
     * @return      A String value.
     */
    public String optString(int index) {
        return optString(index, "");
    }


    /**
     * Get the optional string associated with an index.
     * The defaultValue is returned if the key is not found.
     *
     * @param index The index must be between 0 and length() - 1.
     * @param defaultValue     The default value.
     * @return      A String value.
     */
    public String optString(int index, String defaultValue) {
        Object object = opt(index);
        return object != null ? object.toString() : defaultValue;
    }


    /**
     * Append a boolean value. This increases the array's length by one.
     *
     * @param value A boolean value.
     * @return this.
     */
    public JSONArray put(boolean value) {
        put(value ? Boolean.TRUE : Boolean.FALSE);
        return this;
    }


    /**
     * Put a value in the JSONArray, where the value will be a
     * JSONArray which is produced from a Collection.
     * @param value A Collection value.
     * @return      this.
     */
    public JSONArray put(Collection value) {
        put(new JSONArray(value));
        return this;
    }


    /**
     * Append a double value. This increases the array's length by one.
     *
     * @param value A double value.
     * @throws JSONException if the value is not finite.
     * @return this.
     */
    public JSONArray put(double value) throws JSONException {
        Double d = new Double(value);
        JSONObject.testValidity(d);
        put(d);
        return this;
    }


    /**
     * Append an int value. This increases the array's length by one.
     *
     * @param value An int value.
     * @return this.
     */
    public JSONArray put(int value) {
        put(new Integer(value));
        return this;
    }


    /**
     * Append an long value. This increases the array's length by one.
     *
     * @param value A long value.
     * @return this.
     */
    public JSONArray put(long value) {
        put(new Long(value));
        return this;
    }


    /**
     * Put a value in the JSONArray, where the value will be a
     * JSONObject which is produced from a Map.
     * @param value A Map value.
     * @return      this.
     */
    public JSONArray put(Map value) {
        put(new JSONObject(value));
        return this;
    }


    /**
     * Append an object value. This increases the array's length by one.
     * @param value An object value.  The value should be a
     *  Boolean, Double, Integer, JSONArray, JSONObject, Long, or String, or the
     *  JSONObject.NULL object.
     * @return this.
     */
    public JSONArray put(Object value) {
        this.myArrayList.add(value);
        return this;
    }


    /**
     * Put or replace a boolean value in the JSONArray. If the index is greater
     * than the length of the JSONArray, then null elements will be added as
     * necessary to pad it out.
     * @param index The subscript.
     * @param value A boolean value.
     * @return this.
     * @throws JSONException If the index is negative.
     */
    public JSONArray put(int index, boolean value) throws JSONException {
        put(index, value ? Boolean.TRUE : Boolean.FALSE);
        return this;
    }


    /**
     * Put a value in the JSONArray, where the value will be a
     * JSONArray which is produced from a Collection.
     * @param index The subscript.
     * @param value A Collection value.
     * @return      this.
     * @throws JSONException If the index is negative or if the value is
     * not finite.
     */
    public JSONArray put(int index, Collection value) throws JSONException {
        put(index, new JSONArray(value));
        return this;
    }


    /**
     * Put or replace a double value. If the index is greater than the length of
     *  the JSONArray, then null elements will be added as necessary to pad
     *  it out.
     * @param index The subscript.
     * @param value A double value.
     * @return this.
     * @throws JSONException If the index is negative or if the value is
     * not finite.
     */
    public JSONArray put(int index, double value) throws JSONException {
        put(index, new Double(value));
        return this;
    }


    /**
     * Put or replace an int value. If the index is greater than the length of
     *  the JSONArray, then null elements will be added as necessary to pad
     *  it out.
     * @param index The subscript.
     * @param value An int value.
     * @return this.
     * @throws JSONException If the index is negative.
     */
    public JSONArray put(int index, int value) throws JSONException {
        put(index, new Integer(value));
        return this;
    }


    /**
     * Put or replace a long value. If the index is greater than the length of
     *  the JSONArray, then null elements will be added as necessary to pad
     *  it out.
     * @param index The subscript.
     * @param value A long value.
     * @return this.
     * @throws JSONException If the index is negative.
     */
    public JSONArray put(int index, long value) throws JSONException {
        put(index, new Long(value));
        return this;
    }


    /**
     * Put a value in the JSONArray, where the value will be a
     * JSONObject that is produced from a Map.
     * @param index The subscript.
     * @param value The Map value.
     * @return      this.
     * @throws JSONException If the index is negative or if the the value is
     *  an invalid number.
     */
    public JSONArray put(int index, Map value) throws JSONException {
        put(index, new JSONObject(value));
        return this;
    }


    /**
     * Put or replace an object value in the JSONArray. If the index is greater
     *  than the length of the JSONArray, then null elements will be added as
     *  necessary to pad it out.
     * @param index The subscript.
     * @param value The value to put into the array. The value should be a
     *  Boolean, Double, Integer, JSONArray, JSONObject, Long, or String, or the
     *  JSONObject.NULL object.
     * @return this.
     * @throws JSONException If the index is negative or if the the value is
     *  an invalid number.
     */
    public JSONArray put(int index, Object value) throws JSONException {
        JSONObject.testValidity(value);
        if (index < 0) {
            throw new JSONException("JSONArray[" + index + "] not found.");
        }
        if (index < length()) {
            this.myArrayList.set(index, value);
        } else {
            while (index != length()) {
                put(JSONObject.NULL);
            }
            put(value);
        }
        return this;
    }
    
    
    /**
     * Remove an index and close the hole.
     * @param index The index of the element to be removed.
     * @return The value that was associated with the index,
     * or null if there was no value.
     */
    public Object remove(int index) {
    	Object o = opt(index);
        this.myArrayList.remove(index);
        return o;
    }


    /**
     * Produce a JSONObject by combining a JSONArray of names with the values
     * of this JSONArray.
     * @param names A JSONArray containing a list of key strings. These will be
     * paired with the values.
     * @return A JSONObject, or null if there are no names or if this JSONArray
     * has no values.
     * @throws JSONException If any of the names are null.
     */
    public JSONObject toJSONObject(JSONArray names) throws JSONException {
        if (names == null || names.length() == 0 || length() == 0) {
            return null;
        }
        JSONObject jo = new JSONObject();
        for (int i = 0; i < names.length(); i += 1) {
            jo.put(names.getString(i), this.opt(i));
        }
        return jo;
    }


    /**
     * Make a JSON text of this JSONArray. For compactness, no
     * unnecessary whitespace is added. If it is not possible to produce a
     * syntactically correct JSON text then null will be returned instead. This
     * could occur if the array contains an invalid number.
     * <p>
     * Warning: This method assumes that the data structure is acyclical.
     *
     * @return a printable, displayable, transmittable
     *  representation of the array.
     */
    public String toString() {
        try {
            return '[' + join(",") + ']';
        } catch (Exception e) {
            return null;
        }
    }


    /**
     * Make a prettyprinted JSON text of this JSONArray.
     * Warning: This method assumes that the data structure is acyclical.
     * @param indentFactor The number of spaces to add to each level of
     *  indentation.
     * @return a printable, displayable, transmittable
     *  representation of the object, beginning
     *  with <code>[</code>&nbsp;<small>(left bracket)</small> and ending
     *  with <code>]</code>&nbsp;<small>(right bracket)</small>.
     * @throws JSONException
     */
    public String toString(int indentFactor) throws JSONException {
        return toString(indentFactor, 0);
    }


    /**
     * Make a prettyprinted JSON text of this JSONArray.
     * Warning: This method assumes that the data structure is acyclical.
     * @param indentFactor The number of spaces to add to each level of
     *  indentation.
     * @param indent The indention of the top level.
     * @return a printable, displayable, transmittable
     *  representation of the array.
     * @throws JSONException
     */
    String toString(int indentFactor, int indent) throws JSONException {
        int len = length();
        if (len == 0) {
            return "[]";
        }
        int i;
        StringBuffer sb = new StringBuffer("[");
        if (len == 1) {
            sb.append(JSONObject.valueToString(this.myArrayList.get(0),
                    indentFactor, indent));
        } else {
            int newindent = indent + indentFactor;
            sb.append('\n');
            for (i = 0; i < len; i += 1) {
                if (i > 0) {
                    sb.append(",\n");
                }
                for (int j = 0; j < newindent; j += 1) {
                    sb.append(' ');
                }
                sb.append(JSONObject.valueToString(this.myArrayList.get(i),
                        indentFactor, newindent));
            }
            sb.append('\n');
            for (i = 0; i < indent; i += 1) {
                sb.append(' ');
            }
        }
        sb.append(']');
        return sb.toString();
    }


    /**
     * Write the contents of the JSONArray as JSON text to a writer.
     * For compactness, no whitespace is added.
     * <p>
     * Warning: This method assumes that the data structure is acyclical.
     *
     * @return The writer.
     * @throws JSONException
     */
    public Writer write(Writer writer) throws JSONException {
        try {
            boolean b = false;
            int     len = length();

            writer.write('[');

            for (int i = 0; i < len; i += 1) {
                if (b) {
                    writer.write(',');
                }
                Object v = this.myArrayList.get(i);
                if (v instanceof JSONObject) {
                    ((JSONObject)v).write(writer);
                } else if (v instanceof JSONArray) {
                    ((JSONArray)v).write(writer);
                } else {
                    writer.write(JSONObject.valueToString(v));
                }
                b = true;
            }
            writer.write(']');
            return writer;
        } catch (IOException e) {
           throw new JSONException(e);
        }
    }
}

class JSONException extends Exception {
	private static final long serialVersionUID = 0;
	private Throwable cause;

    /**
     * Constructs a JSONException with an explanatory message.
     * @param message Detail about the reason for the exception.
     */
    public JSONException(String message) {
        super(message);
    }

    public JSONException(Throwable cause) {
        super(cause.getMessage());
        this.cause = cause;
    }

    public Throwable getCause() {
        return this.cause;
    }
}

class JSONObject {

    /**
     * JSONObject.NULL is equivalent to the value that JavaScript calls null,
     * whilst Java's null is equivalent to the value that JavaScript calls
     * undefined.
     */
     private static final class Null {

        /**
         * There is only intended to be a single instance of the NULL object,
         * so the clone method returns itself.
         * @return     NULL.
         */
        protected final Object clone() {
            return this;
        }

        /**
         * A Null object is equal to the null value and to itself.
         * @param object    An object to test for nullness.
         * @return true if the object parameter is the JSONObject.NULL object
         *  or null.
         */
        public boolean equals(Object object) {
            return object == null || object == this;
        }

        /**
         * Get the "null" string value.
         * @return The string "null".
         */
        public String toString() {
            return "null";
        }
    }


    /**
     * The map where the JSONObject's properties are kept.
     */
    private Map map;


    /**
     * It is sometimes more convenient and less ambiguous to have a
     * <code>NULL</code> object than to use Java's <code>null</code> value.
     * <code>JSONObject.NULL.equals(null)</code> returns <code>true</code>.
     * <code>JSONObject.NULL.toString()</code> returns <code>"null"</code>.
     */
    public static final Object NULL = new Null();


    /**
     * Construct an empty JSONObject.
     */
    public JSONObject() {
        this.map = new HashMap();
    }


    /**
     * Construct a JSONObject from a subset of another JSONObject.
     * An array of strings is used to identify the keys that should be copied.
     * Missing keys are ignored.
     * @param jo A JSONObject.
     * @param names An array of strings.
     * @throws JSONException 
     * @exception JSONException If a value is a non-finite number or if a name is duplicated.
     */
    public JSONObject(JSONObject jo, String[] names) {
        this();
        for (int i = 0; i < names.length; i += 1) {
            try {
                putOnce(names[i], jo.opt(names[i]));
            } catch (Exception ignore) {
            }
        }
    }


    /**
     * Construct a JSONObject from a JSONTokener.
     * @param x A JSONTokener object containing the source string.
     * @throws JSONException If there is a syntax error in the source string
     *  or a duplicated key.
     */
    public JSONObject(JSONTokener x) throws JSONException {
        this();
        char c;
        String key;

        if (x.nextClean() != '{') {
            throw x.syntaxError("A JSONObject text must begin with '{'");
        }
        for (;;) {
            c = x.nextClean();
            switch (c) {
            case 0:
                throw x.syntaxError("A JSONObject text must end with '}'");
            case '}':
                return;
            default:
                x.back();
                key = x.nextValue().toString();
            }

// The key is followed by ':'. We will also tolerate '=' or '=>'.

            c = x.nextClean();
            if (c == '=') {
                if (x.next() != '>') {
                    x.back();
                }
            } else if (c != ':') {
                throw x.syntaxError("Expected a ':' after a key");
            }
            putOnce(key, x.nextValue());

// Pairs are separated by ','. We will also tolerate ';'.

            switch (x.nextClean()) {
            case ';':
            case ',':
                if (x.nextClean() == '}') {
                    return;
                }
                x.back();
                break;
            case '}':
                return;
            default:
                throw x.syntaxError("Expected a ',' or '}'");
            }
        }
    }


    /**
     * Construct a JSONObject from a Map.
     *
     * @param map A map object that can be used to initialize the contents of
     *  the JSONObject.
     * @throws JSONException 
     */
    public JSONObject(Map map) {
        this.map = new HashMap();
        if (map != null) {
            Iterator i = map.entrySet().iterator();
            while (i.hasNext()) {
                Map.Entry e = (Map.Entry)i.next();
                Object value = e.getValue();
                if (value != null) {
                    this.map.put(e.getKey(), wrap(value));
                }
            }
        }
    }


    /**
     * Construct a JSONObject from an Object using bean getters.
     * It reflects on all of the public methods of the object.
     * For each of the methods with no parameters and a name starting
     * with <code>"get"</code> or <code>"is"</code> followed by an uppercase letter,
     * the method is invoked, and a key and the value returned from the getter method
     * are put into the new JSONObject.
     *
     * The key is formed by removing the <code>"get"</code> or <code>"is"</code> prefix.
     * If the second remaining character is not upper case, then the first
     * character is converted to lower case.
     *
     * For example, if an object has a method named <code>"getName"</code>, and
     * if the result of calling <code>object.getName()</code> is <code>"Larry Fine"</code>,
     * then the JSONObject will contain <code>"name": "Larry Fine"</code>.
     *
     * @param bean An object that has getter methods that should be used
     * to make a JSONObject.
     */
    public JSONObject(Object bean) {
        this();
        populateMap(bean);
    }


    /**
     * Construct a JSONObject from an Object, using reflection to find the
     * public members. The resulting JSONObject's keys will be the strings
     * from the names array, and the values will be the field values associated
     * with those keys in the object. If a key is not found or not visible,
     * then it will not be copied into the new JSONObject.
     * @param object An object that has fields that should be used to make a
     * JSONObject.
     * @param names An array of strings, the names of the fields to be obtained
     * from the object.
     */
    public JSONObject(Object object, String names[]) {
        this();
        Class c = object.getClass();
        for (int i = 0; i < names.length; i += 1) {
            String name = names[i];
            try {
                putOpt(name, c.getField(name).get(object));
            } catch (Exception ignore) {
            }
        }
    }


    /**
     * Construct a JSONObject from a source JSON text string.
     * This is the most commonly used JSONObject constructor.
     * @param source    A string beginning
     *  with <code>{</code>&nbsp;<small>(left brace)</small> and ending
     *  with <code>}</code>&nbsp;<small>(right brace)</small>.
     * @exception JSONException If there is a syntax error in the source
     *  string or a duplicated key.
     */
    public JSONObject(String source) throws JSONException {
        this(new JSONTokener(source));
    }


    /**
     * Construct a JSONObject from a ResourceBundle.
     * @param baseName The ResourceBundle base name.
     * @param locale The Locale to load the ResourceBundle for.
     * @throws JSONException If any JSONExceptions are detected.
     */
    public JSONObject(String baseName, Locale locale) throws JSONException {
        this();
        ResourceBundle bundle = ResourceBundle.getBundle(baseName, locale, 
                Thread.currentThread().getContextClassLoader());

// Iterate through the keys in the bundle.
        
        Enumeration keys = bundle.getKeys();
        while (keys.hasMoreElements()) {
            Object key = keys.nextElement();
            if (key instanceof String) {
    
// Go through the path, ensuring that there is a nested JSONObject for each 
// segment except the last. Add the value using the last segment's name into
// the deepest nested JSONObject.
                
                String[] path = ((String)key).split("\\.");
                int last = path.length - 1;
                JSONObject target = this;
                for (int i = 0; i < last; i += 1) {
                    String segment = path[i];
                    JSONObject nextTarget = target.optJSONObject(segment);
                    if (nextTarget == null) {
                        nextTarget = new JSONObject();
                        target.put(segment, nextTarget);
                    }
                    target = nextTarget;
                }
                target.put(path[last], bundle.getString((String)key));
            }
        }
    }

    
    /**
     * Accumulate values under a key. It is similar to the put method except
     * that if there is already an object stored under the key then a
     * JSONArray is stored under the key to hold all of the accumulated values.
     * If there is already a JSONArray, then the new value is appended to it.
     * In contrast, the put method replaces the previous value.
     * 
     * If only one value is accumulated that is not a JSONArray, then the
     * result will be the same as using put. But if multiple values are 
     * accumulated, then the result will be like append.
     * @param key   A key string.
     * @param value An object to be accumulated under the key.
     * @return this.
     * @throws JSONException If the value is an invalid number
     *  or if the key is null.
     */
    public JSONObject accumulate(
        String key, 
        Object value
    ) throws JSONException {
        testValidity(value);
        Object object = opt(key);
        if (object == null) {
            put(key, value instanceof JSONArray ?
                    new JSONArray().put(value) : value);
        } else if (object instanceof JSONArray) {
            ((JSONArray)object).put(value);
        } else {
            put(key, new JSONArray().put(object).put(value));
        }
        return this;
    }


    /**
     * Append values to the array under a key. If the key does not exist in the
     * JSONObject, then the key is put in the JSONObject with its value being a
     * JSONArray containing the value parameter. If the key was already
     * associated with a JSONArray, then the value parameter is appended to it.
     * @param key   A key string.
     * @param value An object to be accumulated under the key.
     * @return this.
     * @throws JSONException If the key is null or if the current value
     *  associated with the key is not a JSONArray.
     */
    public JSONObject append(String key, Object value) throws JSONException {
        testValidity(value);
        Object object = opt(key);
        if (object == null) {
            put(key, new JSONArray().put(value));
        } else if (object instanceof JSONArray) {
            put(key, ((JSONArray)object).put(value));
        } else {
            throw new JSONException("JSONObject[" + key +
                    "] is not a JSONArray.");
        }
        return this;
    }


    /**
     * Produce a string from a double. The string "null" will be returned if
     * the number is not finite.
     * @param  d A double.
     * @return A String.
     */
    public static String doubleToString(double d) {
        if (Double.isInfinite(d) || Double.isNaN(d)) {
            return "null";
        }

// Shave off trailing zeros and decimal point, if possible.

        String string = Double.toString(d);
        if (string.indexOf('.') > 0 && string.indexOf('e') < 0 && 
        		string.indexOf('E') < 0) {
            while (string.endsWith("0")) {
                string = string.substring(0, string.length() - 1);
            }
            if (string.endsWith(".")) {
                string = string.substring(0, string.length() - 1);
            }
        }
        return string;
    }


    /**
     * Get the value object associated with a key.
     *
     * @param key   A key string.
     * @return      The object associated with the key.
     * @throws      JSONException if the key is not found.
     */
    public Object get(String key) throws JSONException {
        if (key == null) {
            throw new JSONException("Null key.");
        }
        Object object = opt(key);
        if (object == null) {
            throw new JSONException("JSONObject[" + quote(key) +
                    "] not found.");
        }
        return object;
    }


    /**
     * Get the boolean value associated with a key.
     *
     * @param key   A key string.
     * @return      The truth.
     * @throws      JSONException
     *  if the value is not a Boolean or the String "true" or "false".
     */
    public boolean getBoolean(String key) throws JSONException {
        Object object = get(key);
        if (object.equals(Boolean.FALSE) ||
                (object instanceof String &&
                ((String)object).equalsIgnoreCase("false"))) {
            return false;
        } else if (object.equals(Boolean.TRUE) ||
                (object instanceof String &&
                ((String)object).equalsIgnoreCase("true"))) {
            return true;
        }
        throw new JSONException("JSONObject[" + quote(key) +
                "] is not a Boolean.");
    }


    /**
     * Get the double value associated with a key.
     * @param key   A key string.
     * @return      The numeric value.
     * @throws JSONException if the key is not found or
     *  if the value is not a Number object and cannot be converted to a number.
     */
    public double getDouble(String key) throws JSONException {
        Object object = get(key);
        try {
            return object instanceof Number ?
                ((Number)object).doubleValue() :
                Double.parseDouble((String)object);
        } catch (Exception e) {
            throw new JSONException("JSONObject[" + quote(key) +
                "] is not a number.");
        }
    }


    /**
     * Get the int value associated with a key. 
     *
     * @param key   A key string.
     * @return      The integer value.
     * @throws   JSONException if the key is not found or if the value cannot
     *  be converted to an integer.
     */
    public int getInt(String key) throws JSONException {
        Object object = get(key);
        try {
            return object instanceof Number ?
                ((Number)object).intValue() :
                Integer.parseInt((String)object);
        } catch (Exception e) {
            throw new JSONException("JSONObject[" + quote(key) +
                "] is not an int.");
        }
    }


    /**
     * Get the JSONArray value associated with a key.
     *
     * @param key   A key string.
     * @return      A JSONArray which is the value.
     * @throws      JSONException if the key is not found or
     *  if the value is not a JSONArray.
     */
    public JSONArray getJSONArray(String key) throws JSONException {
        Object object = get(key);
        if (object instanceof JSONArray) {
            return (JSONArray)object;
        }
        throw new JSONException("JSONObject[" + quote(key) +
                "] is not a JSONArray.");
    }


    /**
     * Get the JSONObject value associated with a key.
     *
     * @param key   A key string.
     * @return      A JSONObject which is the value.
     * @throws      JSONException if the key is not found or
     *  if the value is not a JSONObject.
     */
    public JSONObject getJSONObject(String key) throws JSONException {
        Object object = get(key);
        if (object instanceof JSONObject) {
            return (JSONObject)object;
        }
        throw new JSONException("JSONObject[" + quote(key) +
                "] is not a JSONObject.");
    }


    /**
     * Get the long value associated with a key. 
     *
     * @param key   A key string.
     * @return      The long value.
     * @throws   JSONException if the key is not found or if the value cannot
     *  be converted to a long.
     */
    public long getLong(String key) throws JSONException {
        Object object = get(key);
        try {
            return object instanceof Number ?
                ((Number)object).longValue() :
                Long.parseLong((String)object);
        } catch (Exception e) {
            throw new JSONException("JSONObject[" + quote(key) +
                "] is not a long.");
        }
    }


    /**
     * Get an array of field names from a JSONObject.
     *
     * @return An array of field names, or null if there are no names.
     */
    public static String[] getNames(JSONObject jo) {
        int length = jo.length();
        if (length == 0) {
            return null;
        }
        Iterator iterator = jo.keys();
        String[] names = new String[length];
        int i = 0;
        while (iterator.hasNext()) {
            names[i] = (String)iterator.next();
            i += 1;
        }
        return names;
    }


    /**
     * Get an array of field names from an Object.
     *
     * @return An array of field names, or null if there are no names.
     */
    public static String[] getNames(Object object) {
        if (object == null) {
            return null;
        }
        Class klass = object.getClass();
        Field[] fields = klass.getFields();
        int length = fields.length;
        if (length == 0) {
            return null;
        }
        String[] names = new String[length];
        for (int i = 0; i < length; i += 1) {
            names[i] = fields[i].getName();
        }
        return names;
    }


    /**
     * Get the string associated with a key.
     *
     * @param key   A key string.
     * @return      A string which is the value.
     * @throws   JSONException if there is no string value for the key.
     */
    public String getString(String key) throws JSONException {
        Object object = get(key);
        if (object instanceof String) {
            return (String)object;
        }
        throw new JSONException("JSONObject[" + quote(key) +
            "] not a string.");
    }


    /**
     * Determine if the JSONObject contains a specific key.
     * @param key   A key string.
     * @return      true if the key exists in the JSONObject.
     */
    public boolean has(String key) {
        return this.map.containsKey(key);
    }
    
    
    /**
     * Increment a property of a JSONObject. If there is no such property,
     * create one with a value of 1. If there is such a property, and if
     * it is an Integer, Long, Double, or Float, then add one to it.
     * @param key  A key string.
     * @return this.
     * @throws JSONException If there is already a property with this name
     * that is not an Integer, Long, Double, or Float.
     */
    public JSONObject increment(String key) throws JSONException {
        Object value = opt(key);
        if (value == null) {
            put(key, 1);
        } else if (value instanceof Integer) {
            put(key, ((Integer)value).intValue() + 1);
        } else if (value instanceof Long) {
            put(key, ((Long)value).longValue() + 1);                
        } else if (value instanceof Double) {
            put(key, ((Double)value).doubleValue() + 1);                
        } else if (value instanceof Float) {
            put(key, ((Float)value).floatValue() + 1);                
        } else {
            throw new JSONException("Unable to increment [" + quote(key) + "].");
        }
        return this;
    }


    /**
     * Determine if the value associated with the key is null or if there is
     *  no value.
     * @param key   A key string.
     * @return      true if there is no value associated with the key or if
     *  the value is the JSONObject.NULL object.
     */
    public boolean isNull(String key) {
        return JSONObject.NULL.equals(opt(key));
    }


    /**
     * Get an enumeration of the keys of the JSONObject.
     *
     * @return An iterator of the keys.
     */
    public Iterator keys() {
        return this.map.keySet().iterator();
    }


    /**
     * Get the number of keys stored in the JSONObject.
     *
     * @return The number of keys in the JSONObject.
     */
    public int length() {
        return this.map.size();
    }


    /**
     * Produce a JSONArray containing the names of the elements of this
     * JSONObject.
     * @return A JSONArray containing the key strings, or null if the JSONObject
     * is empty.
     */
    public JSONArray names() {
        JSONArray ja = new JSONArray();
        Iterator  keys = this.keys();
        while (keys.hasNext()) {
            ja.put(keys.next());
        }
        return ja.length() == 0 ? null : ja;
    }

    /**
     * Produce a string from a Number.
     * @param  number A Number
     * @return A String.
     * @throws JSONException If n is a non-finite number.
     */
    public static String numberToString(Number number)
            throws JSONException {
        if (number == null) {
            throw new JSONException("Null pointer");
        }
        testValidity(number);

// Shave off trailing zeros and decimal point, if possible.

        String string = number.toString();
        if (string.indexOf('.') > 0 && string.indexOf('e') < 0 && 
        		string.indexOf('E') < 0) {
            while (string.endsWith("0")) {
                string = string.substring(0, string.length() - 1);
            }
            if (string.endsWith(".")) {
                string = string.substring(0, string.length() - 1);
            }
        }
        return string;
    }


    /**
     * Get an optional value associated with a key.
     * @param key   A key string.
     * @return      An object which is the value, or null if there is no value.
     */
    public Object opt(String key) {
        return key == null ? null : this.map.get(key);
    }


    /**
     * Get an optional boolean associated with a key.
     * It returns false if there is no such key, or if the value is not
     * Boolean.TRUE or the String "true".
     *
     * @param key   A key string.
     * @return      The truth.
     */
    public boolean optBoolean(String key) {
        return optBoolean(key, false);
    }


    /**
     * Get an optional boolean associated with a key.
     * It returns the defaultValue if there is no such key, or if it is not
     * a Boolean or the String "true" or "false" (case insensitive).
     *
     * @param key              A key string.
     * @param defaultValue     The default.
     * @return      The truth.
     */
    public boolean optBoolean(String key, boolean defaultValue) {
        try {
            return getBoolean(key);
        } catch (Exception e) {
            return defaultValue;
        }
    }


    /**
     * Get an optional double associated with a key,
     * or NaN if there is no such key or if its value is not a number.
     * If the value is a string, an attempt will be made to evaluate it as
     * a number.
     *
     * @param key   A string which is the key.
     * @return      An object which is the value.
     */
    public double optDouble(String key) {
        return optDouble(key, Double.NaN);
    }


    /**
     * Get an optional double associated with a key, or the
     * defaultValue if there is no such key or if its value is not a number.
     * If the value is a string, an attempt will be made to evaluate it as
     * a number.
     *
     * @param key   A key string.
     * @param defaultValue     The default.
     * @return      An object which is the value.
     */
    public double optDouble(String key, double defaultValue) {
        try {
            return getDouble(key);
        } catch (Exception e) {
            return defaultValue;
        }
    }


    /**
     * Get an optional int value associated with a key,
     * or zero if there is no such key or if the value is not a number.
     * If the value is a string, an attempt will be made to evaluate it as
     * a number.
     *
     * @param key   A key string.
     * @return      An object which is the value.
     */
    public int optInt(String key) {
        return optInt(key, 0);
    }


    /**
     * Get an optional int value associated with a key,
     * or the default if there is no such key or if the value is not a number.
     * If the value is a string, an attempt will be made to evaluate it as
     * a number.
     *
     * @param key   A key string.
     * @param defaultValue     The default.
     * @return      An object which is the value.
     */
    public int optInt(String key, int defaultValue) {
        try {
            return getInt(key);
        } catch (Exception e) {
            return defaultValue;
        }
    }


    /**
     * Get an optional JSONArray associated with a key.
     * It returns null if there is no such key, or if its value is not a
     * JSONArray.
     *
     * @param key   A key string.
     * @return      A JSONArray which is the value.
     */
    public JSONArray optJSONArray(String key) {
        Object o = opt(key);
        return o instanceof JSONArray ? (JSONArray)o : null;
    }


    /**
     * Get an optional JSONObject associated with a key.
     * It returns null if there is no such key, or if its value is not a
     * JSONObject.
     *
     * @param key   A key string.
     * @return      A JSONObject which is the value.
     */
    public JSONObject optJSONObject(String key) {
        Object object = opt(key);
        return object instanceof JSONObject ? (JSONObject)object : null;
    }


    /**
     * Get an optional long value associated with a key,
     * or zero if there is no such key or if the value is not a number.
     * If the value is a string, an attempt will be made to evaluate it as
     * a number.
     *
     * @param key   A key string.
     * @return      An object which is the value.
     */
    public long optLong(String key) {
        return optLong(key, 0);
    }


    /**
     * Get an optional long value associated with a key,
     * or the default if there is no such key or if the value is not a number.
     * If the value is a string, an attempt will be made to evaluate it as
     * a number.
     *
     * @param key          A key string.
     * @param defaultValue The default.
     * @return             An object which is the value.
     */
    public long optLong(String key, long defaultValue) {
        try {
            return getLong(key);
        } catch (Exception e) {
            return defaultValue;
        }
    }


    /**
     * Get an optional string associated with a key.
     * It returns an empty string if there is no such key. If the value is not
     * a string and is not null, then it is converted to a string.
     *
     * @param key   A key string.
     * @return      A string which is the value.
     */
    public String optString(String key) {
        return optString(key, "");
    }


    /**
     * Get an optional string associated with a key.
     * It returns the defaultValue if there is no such key.
     *
     * @param key   A key string.
     * @param defaultValue     The default.
     * @return      A string which is the value.
     */
    public String optString(String key, String defaultValue) {
        Object object = opt(key);
        return NULL.equals(object) ? defaultValue : object.toString();        
    }


    private void populateMap(Object bean) {
        Class klass = bean.getClass();

// If klass is a System class then set includeSuperClass to false. 

        boolean includeSuperClass = klass.getClassLoader() != null;

        Method[] methods = (includeSuperClass) ?
                klass.getMethods() : klass.getDeclaredMethods();
        for (int i = 0; i < methods.length; i += 1) {
            try {
                Method method = methods[i];
                if (Modifier.isPublic(method.getModifiers())) {
                    String name = method.getName();
                    String key = "";
                    if (name.startsWith("get")) {
                        if (name.equals("getClass") || 
                                name.equals("getDeclaringClass")) {
                            key = "";
                        } else {
                            key = name.substring(3);
                        }
                    } else if (name.startsWith("is")) {
                        key = name.substring(2);
                    }
                    if (key.length() > 0 &&
                            Character.isUpperCase(key.charAt(0)) &&
                            method.getParameterTypes().length == 0) {
                        if (key.length() == 1) {
                            key = key.toLowerCase();
                        } else if (!Character.isUpperCase(key.charAt(1))) {
                            key = key.substring(0, 1).toLowerCase() +
                                key.substring(1);
                        }

                        Object result = method.invoke(bean, (Object[])null);
                        if (result != null) {
                            map.put(key, wrap(result));
                        }
                    }
                }
            } catch (Exception ignore) {
            }
        }
    }


    /**
     * Put a key/boolean pair in the JSONObject.
     *
     * @param key   A key string.
     * @param value A boolean which is the value.
     * @return this.
     * @throws JSONException If the key is null.
     */
    public JSONObject put(String key, boolean value) throws JSONException {
        put(key, value ? Boolean.TRUE : Boolean.FALSE);
        return this;
    }


    /**
     * Put a key/value pair in the JSONObject, where the value will be a
     * JSONArray which is produced from a Collection.
     * @param key   A key string.
     * @param value A Collection value.
     * @return      this.
     * @throws JSONException
     */
    public JSONObject put(String key, Collection value) throws JSONException {
        put(key, new JSONArray(value));
        return this;
    }


    /**
     * Put a key/double pair in the JSONObject.
     *
     * @param key   A key string.
     * @param value A double which is the value.
     * @return this.
     * @throws JSONException If the key is null or if the number is invalid.
     */
    public JSONObject put(String key, double value) throws JSONException {
        put(key, new Double(value));
        return this;
    }


    /**
     * Put a key/int pair in the JSONObject.
     *
     * @param key   A key string.
     * @param value An int which is the value.
     * @return this.
     * @throws JSONException If the key is null.
     */
    public JSONObject put(String key, int value) throws JSONException {
        put(key, new Integer(value));
        return this;
    }


    /**
     * Put a key/long pair in the JSONObject.
     *
     * @param key   A key string.
     * @param value A long which is the value.
     * @return this.
     * @throws JSONException If the key is null.
     */
    public JSONObject put(String key, long value) throws JSONException {
        put(key, new Long(value));
        return this;
    }


    /**
     * Put a key/value pair in the JSONObject, where the value will be a
     * JSONObject which is produced from a Map.
     * @param key   A key string.
     * @param value A Map value.
     * @return      this.
     * @throws JSONException
     */
    public JSONObject put(String key, Map value) throws JSONException {
        put(key, new JSONObject(value));
        return this;
    }


    /**
     * Put a key/value pair in the JSONObject. If the value is null,
     * then the key will be removed from the JSONObject if it is present.
     * @param key   A key string.
     * @param value An object which is the value. It should be of one of these
     *  types: Boolean, Double, Integer, JSONArray, JSONObject, Long, String,
     *  or the JSONObject.NULL object.
     * @return this.
     * @throws JSONException If the value is non-finite number
     *  or if the key is null.
     */
    public JSONObject put(String key, Object value) throws JSONException {
        if (key == null) {
            throw new JSONException("Null key.");
        }
        if (value != null) {
            testValidity(value);
            this.map.put(key, value);
        } else {
            remove(key);
        }
        return this;
    }


    /**
     * Put a key/value pair in the JSONObject, but only if the key and the
     * value are both non-null, and only if there is not already a member
     * with that name.
     * @param key
     * @param value
     * @return his.
     * @throws JSONException if the key is a duplicate
     */
    public JSONObject putOnce(String key, Object value) throws JSONException {
        if (key != null && value != null) {
            if (opt(key) != null) {
                throw new JSONException("Duplicate key \"" + key + "\"");
            }
            put(key, value);
        }
        return this;
    }


    /**
     * Put a key/value pair in the JSONObject, but only if the
     * key and the value are both non-null.
     * @param key   A key string.
     * @param value An object which is the value. It should be of one of these
     *  types: Boolean, Double, Integer, JSONArray, JSONObject, Long, String,
     *  or the JSONObject.NULL object.
     * @return this.
     * @throws JSONException If the value is a non-finite number.
     */
    public JSONObject putOpt(String key, Object value) throws JSONException {
        if (key != null && value != null) {
            put(key, value);
        }
        return this;
    }


    /**
     * Produce a string in double quotes with backslash sequences in all the
     * right places. A backslash will be inserted within </, producing <\/,
     * allowing JSON text to be delivered in HTML. In JSON text, a string 
     * cannot contain a control character or an unescaped quote or backslash.
     * @param string A String
     * @return  A String correctly formatted for insertion in a JSON text.
     */
    public static String quote(String string) {
        if (string == null || string.length() == 0) {
            return "\"\"";
        }

        char         b;
        char         c = 0;
        String       hhhh;
        int          i;
        int          len = string.length();
        StringBuffer sb = new StringBuffer(len + 4);

        sb.append('"');
        for (i = 0; i < len; i += 1) {
            b = c;
            c = string.charAt(i);
            switch (c) {
            case '\\':
            case '"':
                sb.append('\\');
                sb.append(c);
                break;
            case '/':
                if (b == '<') {
                    sb.append('\\');
                }
                sb.append(c);
                break;
            case '\b':
                sb.append("\\b");
                break;
            case '\t':
                sb.append("\\t");
                break;
            case '\n':
                sb.append("\\n");
                break;
            case '\f':
                sb.append("\\f");
                break;
            case '\r':
                sb.append("\\r");
                break;
            default:
                if (c < ' ' || (c >= '\u0080' && c < '\u00a0') ||
                               (c >= '\u2000' && c < '\u2100')) {
                    hhhh = "000" + Integer.toHexString(c);
                    sb.append("\\u" + hhhh.substring(hhhh.length() - 4));
                } else {
                    sb.append(c);
                }
            }
        }
        sb.append('"');
        return sb.toString();
    }

    /**
     * Remove a name and its value, if present.
     * @param key The name to be removed.
     * @return The value that was associated with the name,
     * or null if there was no value.
     */
    public Object remove(String key) {
        return this.map.remove(key);
    }

    /**
     * Try to convert a string into a number, boolean, or null. If the string
     * can't be converted, return the string.
     * @param string A String.
     * @return A simple JSON value.
     */
    public static Object stringToValue(String string) {
        if (string.equals("")) {
            return string;
        }
        if (string.equalsIgnoreCase("true")) {
            return Boolean.TRUE;
        }
        if (string.equalsIgnoreCase("false")) {
            return Boolean.FALSE;
        }
        if (string.equalsIgnoreCase("null")) {
            return JSONObject.NULL;
        }

        /*
         * If it might be a number, try converting it. 
         * We support the non-standard 0x- convention. 
         * If a number cannot be produced, then the value will just
         * be a string. Note that the 0x-, plus, and implied string
         * conventions are non-standard. A JSON parser may accept
         * non-JSON forms as long as it accepts all correct JSON forms.
         */

        char b = string.charAt(0);
        if ((b >= '0' && b <= '9') || b == '.' || b == '-' || b == '+') {
            if (b == '0' && string.length() > 2 &&
                        (string.charAt(1) == 'x' || string.charAt(1) == 'X')) {
                try {
                    return new Integer(Integer.parseInt(string.substring(2), 16));
                } catch (Exception ignore) {
                }
            }
            try {
                if (string.indexOf('.') > -1 || 
                        string.indexOf('e') > -1 || string.indexOf('E') > -1) {
                    return Double.valueOf(string);
                } else {
                    Long myLong = new Long(string);
                    if (myLong.longValue() == myLong.intValue()) {
                        return new Integer(myLong.intValue());
                    } else {
                        return myLong;
                    }
                }
            }  catch (Exception ignore) {
            }
        }
        return string;
    }


    /**
     * Throw an exception if the object is a NaN or infinite number.
     * @param o The object to test.
     * @throws JSONException If o is a non-finite number.
     */
    public static void testValidity(Object o) throws JSONException {
        if (o != null) {
            if (o instanceof Double) {
                if (((Double)o).isInfinite() || ((Double)o).isNaN()) {
                    throw new JSONException(
                        "JSON does not allow non-finite numbers.");
                }
            } else if (o instanceof Float) {
                if (((Float)o).isInfinite() || ((Float)o).isNaN()) {
                    throw new JSONException(
                        "JSON does not allow non-finite numbers.");
                }
            }
        }
    }


    /**
     * Produce a JSONArray containing the values of the members of this
     * JSONObject.
     * @param names A JSONArray containing a list of key strings. This
     * determines the sequence of the values in the result.
     * @return A JSONArray of values.
     * @throws JSONException If any of the values are non-finite numbers.
     */
    public JSONArray toJSONArray(JSONArray names) throws JSONException {
        if (names == null || names.length() == 0) {
            return null;
        }
        JSONArray ja = new JSONArray();
        for (int i = 0; i < names.length(); i += 1) {
            ja.put(this.opt(names.getString(i)));
        }
        return ja;
    }

    /**
     * Make a JSON text of this JSONObject. For compactness, no whitespace
     * is added. If this would not result in a syntactically correct JSON text,
     * then null will be returned instead.
     * <p>
     * Warning: This method assumes that the data structure is acyclical.
     *
     * @return a printable, displayable, portable, transmittable
     *  representation of the object, beginning
     *  with <code>{</code>&nbsp;<small>(left brace)</small> and ending
     *  with <code>}</code>&nbsp;<small>(right brace)</small>.
     */
    public String toString() {
        try {
            Iterator     keys = this.keys();
            StringBuffer sb = new StringBuffer("{");

            while (keys.hasNext()) {
                if (sb.length() > 1) {
                    sb.append(',');
                }
                Object o = keys.next();
                sb.append(quote(o.toString()));
                sb.append(':');
                sb.append(valueToString(this.map.get(o)));
            }
            sb.append('}');
            return sb.toString();
        } catch (Exception e) {
            return null;
        }
    }


    /**
     * Make a prettyprinted JSON text of this JSONObject.
     * <p>
     * Warning: This method assumes that the data structure is acyclical.
     * @param indentFactor The number of spaces to add to each level of
     *  indentation.
     * @return a printable, displayable, portable, transmittable
     *  representation of the object, beginning
     *  with <code>{</code>&nbsp;<small>(left brace)</small> and ending
     *  with <code>}</code>&nbsp;<small>(right brace)</small>.
     * @throws JSONException If the object contains an invalid number.
     */
    public String toString(int indentFactor) throws JSONException {
        return toString(indentFactor, 0);
    }


    /**
     * Make a prettyprinted JSON text of this JSONObject.
     * <p>
     * Warning: This method assumes that the data structure is acyclical.
     * @param indentFactor The number of spaces to add to each level of
     *  indentation.
     * @param indent The indentation of the top level.
     * @return a printable, displayable, transmittable
     *  representation of the object, beginning
     *  with <code>{</code>&nbsp;<small>(left brace)</small> and ending
     *  with <code>}</code>&nbsp;<small>(right brace)</small>.
     * @throws JSONException If the object contains an invalid number.
     */
    String toString(int indentFactor, int indent) throws JSONException {
        int i;
        int length = this.length();
        if (length == 0) {
            return "{}";
        }
        Iterator     keys = this.keys();
        int          newindent = indent + indentFactor;
        Object       object;
        StringBuffer sb = new StringBuffer("{");
        if (length == 1) {
            object = keys.next();
            sb.append(quote(object.toString()));
            sb.append(": ");
            sb.append(valueToString(this.map.get(object), indentFactor,
                    indent));
        } else {
            while (keys.hasNext()) {
                object = keys.next();
                if (sb.length() > 1) {
                    sb.append(",\n");
                } else {
                    sb.append('\n');
                }
                for (i = 0; i < newindent; i += 1) {
                    sb.append(' ');
                }
                sb.append(quote(object.toString()));
                sb.append(": ");
                sb.append(valueToString(this.map.get(object), indentFactor,
                        newindent));
            }
            if (sb.length() > 1) {
                sb.append('\n');
                for (i = 0; i < indent; i += 1) {
                    sb.append(' ');
                }
            }
        }
        sb.append('}');
        return sb.toString();
    }


    /**
     * Make a JSON text of an Object value. If the object has an
     * value.toJSONString() method, then that method will be used to produce
     * the JSON text. The method is required to produce a strictly
     * conforming text. If the object does not contain a toJSONString
     * method (which is the most common case), then a text will be
     * produced by other means. If the value is an array or Collection,
     * then a JSONArray will be made from it and its toJSONString method
     * will be called. If the value is a MAP, then a JSONObject will be made
     * from it and its toJSONString method will be called. Otherwise, the
     * value's toString method will be called, and the result will be quoted.
     *
     * <p>
     * Warning: This method assumes that the data structure is acyclical.
     * @param value The value to be serialized.
     * @return a printable, displayable, transmittable
     *  representation of the object, beginning
     *  with <code>{</code>&nbsp;<small>(left brace)</small> and ending
     *  with <code>}</code>&nbsp;<small>(right brace)</small>.
     * @throws JSONException If the value is or contains an invalid number.
     */
    public static String valueToString(Object value) throws JSONException {
        if (value == null || value.equals(null)) {
            return "null";
        }
        if (value instanceof JSONString) {
            Object object;
            try {
                object = ((JSONString)value).toJSONString();
            } catch (Exception e) {
                throw new JSONException(e);
            }
            if (object instanceof String) {
                return (String)object;
            }
            throw new JSONException("Bad value from toJSONString: " + object);
        }
        if (value instanceof Number) {
            return numberToString((Number) value);
        }
        if (value instanceof Boolean || value instanceof JSONObject ||
                value instanceof JSONArray) {
            return value.toString();
        }
        if (value instanceof Map) {
            return new JSONObject((Map)value).toString();
        }
        if (value instanceof Collection) {
            return new JSONArray((Collection)value).toString();
        }
        if (value.getClass().isArray()) {
            return new JSONArray(value).toString();
        }
        return quote(value.toString());
    }


    /**
     * Make a prettyprinted JSON text of an object value.
     * <p>
     * Warning: This method assumes that the data structure is acyclical.
     * @param value The value to be serialized.
     * @param indentFactor The number of spaces to add to each level of
     *  indentation.
     * @param indent The indentation of the top level.
     * @return a printable, displayable, transmittable
     *  representation of the object, beginning
     *  with <code>{</code>&nbsp;<small>(left brace)</small> and ending
     *  with <code>}</code>&nbsp;<small>(right brace)</small>.
     * @throws JSONException If the object contains an invalid number.
     */
     static String valueToString(
         Object value, 
         int    indentFactor, 
         int    indent
     ) throws JSONException {
        if (value == null || value.equals(null)) {
            return "null";
        }
        try {
            if (value instanceof JSONString) {
                Object o = ((JSONString)value).toJSONString();
                if (o instanceof String) {
                    return (String)o;
                }
            }
        } catch (Exception ignore) {
        }
        if (value instanceof Number) {
            return numberToString((Number) value);
        }
        if (value instanceof Boolean) {
            return value.toString();
        }
        if (value instanceof JSONObject) {
            return ((JSONObject)value).toString(indentFactor, indent);
        }
        if (value instanceof JSONArray) {
            return ((JSONArray)value).toString(indentFactor, indent);
        }
        if (value instanceof Map) {
            return new JSONObject((Map)value).toString(indentFactor, indent);
        }
        if (value instanceof Collection) {
            return new JSONArray((Collection)value).toString(indentFactor, indent);
        }
        if (value.getClass().isArray()) {
            return new JSONArray(value).toString(indentFactor, indent);
        }
        return quote(value.toString());
    }


     /**
      * Wrap an object, if necessary. If the object is null, return the NULL 
      * object. If it is an array or collection, wrap it in a JSONArray. If 
      * it is a map, wrap it in a JSONObject. If it is a standard property 
      * (Double, String, et al) then it is already wrapped. Otherwise, if it 
      * comes from one of the java packages, turn it into a string. And if 
      * it doesn't, try to wrap it in a JSONObject. If the wrapping fails,
      * then null is returned.
      *
      * @param object The object to wrap
      * @return The wrapped value
      */
     public static Object wrap(Object object) {
         try {
             if (object == null) {
                 return NULL;
             }
             if (object instanceof JSONObject || object instanceof JSONArray  || 
                     NULL.equals(object)      || object instanceof JSONString || 
                     object instanceof Byte   || object instanceof Character  ||
                     object instanceof Short  || object instanceof Integer    ||
                     object instanceof Long   || object instanceof Boolean    || 
                     object instanceof Float  || object instanceof Double     ||
                     object instanceof String) {
                 return object;
             }
             
             if (object instanceof Collection) {
                 return new JSONArray((Collection)object);
             }
             if (object.getClass().isArray()) {
                 return new JSONArray(object);
             }
             if (object instanceof Map) {
                 return new JSONObject((Map)object);
             }
             Package objectPackage = object.getClass().getPackage();
             String objectPackageName = objectPackage != null ? 
                 objectPackage.getName() : "";
             if (
                 objectPackageName.startsWith("java.") ||
                 objectPackageName.startsWith("javax.") ||
                 object.getClass().getClassLoader() == null
             ) {
                 return object.toString();
             }
             return new JSONObject(object);
         } catch(Exception exception) {
             return null;
         }
     }

     
     /**
      * Write the contents of the JSONObject as JSON text to a writer.
      * For compactness, no whitespace is added.
      * <p>
      * Warning: This method assumes that the data structure is acyclical.
      *
      * @return The writer.
      * @throws JSONException
      */
     public Writer write(Writer writer) throws JSONException {
        try {
            boolean  commanate = false;
            Iterator keys = this.keys();
            writer.write('{');

            while (keys.hasNext()) {
                if (commanate) {
                    writer.write(',');
                }
                Object key = keys.next();
                writer.write(quote(key.toString()));
                writer.write(':');
                Object value = this.map.get(key);
                if (value instanceof JSONObject) {
                    ((JSONObject)value).write(writer);
                } else if (value instanceof JSONArray) {
                    ((JSONArray)value).write(writer);
                } else {
                    writer.write(valueToString(value));
                }
                commanate = true;
            }
            writer.write('}');
            return writer;
        } catch (IOException exception) {
            throw new JSONException(exception);
        }
     }
}

interface JSONString {
	/**
	 * The <code>toJSONString</code> method allows a class to produce its own JSON 
	 * serialization. 
	 * 
	 * @return A strictly syntactically correct JSON text.
	 */
	public String toJSONString();
}

class JSONTokener {

    private int 	character;
	private boolean eof;
    private int 	index;
    private int 	line;
    private char 	previous;
    private Reader 	reader;
    private boolean usePrevious;


    /**
     * Construct a JSONTokener from a Reader.
     *
     * @param reader     A reader.
     */
    public JSONTokener(Reader reader) {
        this.reader = reader.markSupported() ? 
        		reader : new BufferedReader(reader);
        this.eof = false;
        this.usePrevious = false;
        this.previous = 0;
        this.index = 0;
        this.character = 1;
        this.line = 1;
    }
    
    
    /**
     * Construct a JSONTokener from an InputStream.
     */
    public JSONTokener(InputStream inputStream) throws JSONException {
        this(new InputStreamReader(inputStream));    	
    }


    /**
     * Construct a JSONTokener from a string.
     *
     * @param s     A source string.
     */
    public JSONTokener(String s) {
        this(new StringReader(s));
    }


    /**
     * Back up one character. This provides a sort of lookahead capability,
     * so that you can test for a digit or letter before attempting to parse
     * the next number or identifier.
     */
    public void back() throws JSONException {
        if (usePrevious || index <= 0) {
            throw new JSONException("Stepping back two steps is not supported");
        }
        this.index -= 1;
        this.character -= 1;
        this.usePrevious = true;
        this.eof = false;
    }


    /**
     * Get the hex value of a character (base16).
     * @param c A character between '0' and '9' or between 'A' and 'F' or
     * between 'a' and 'f'.
     * @return  An int between 0 and 15, or -1 if c was not a hex digit.
     */
    public static int dehexchar(char c) {
        if (c >= '0' && c <= '9') {
            return c - '0';
        }
        if (c >= 'A' && c <= 'F') {
            return c - ('A' - 10);
        }
        if (c >= 'a' && c <= 'f') {
            return c - ('a' - 10);
        }
        return -1;
    }
    
    public boolean end() {
    	return eof && !usePrevious;    	
    }


    /**
     * Determine if the source string still contains characters that next()
     * can consume.
     * @return true if not yet at the end of the source.
     */
    public boolean more() throws JSONException {
        next();
        if (end()) {
            return false;
        } 
        back();
        return true;
    }


    /**
     * Get the next character in the source string.
     *
     * @return The next character, or 0 if past the end of the source string.
     */
    public char next() throws JSONException {
        int c;
        if (this.usePrevious) {
        	this.usePrevious = false;
            c = this.previous;
        } else {
	        try {
	            c = this.reader.read();
	        } catch (IOException exception) {
	            throw new JSONException(exception);
	        }
	
	        if (c <= 0) { // End of stream
	        	this.eof = true;
	        	c = 0;
	        } 
        }
    	this.index += 1;
    	if (this.previous == '\r') {
    		this.line += 1;
    		this.character = c == '\n' ? 0 : 1;
    	} else if (c == '\n') {
    		this.line += 1;
    		this.character = 0;
    	} else {
    		this.character += 1;
    	}
    	this.previous = (char) c;
        return this.previous;
    }


    /**
     * Consume the next character, and check that it matches a specified
     * character.
     * @param c The character to match.
     * @return The character.
     * @throws JSONException if the character does not match.
     */
    public char next(char c) throws JSONException {
        char n = next();
        if (n != c) {
            throw syntaxError("Expected '" + c + "' and instead saw '" +
                    n + "'");
        }
        return n;
    }


    /**
     * Get the next n characters.
     *
     * @param n     The number of characters to take.
     * @return      A string of n characters.
     * @throws JSONException
     *   Substring bounds error if there are not
     *   n characters remaining in the source string.
     */
     public String next(int n) throws JSONException {
         if (n == 0) {
             return "";
         }

         char[] chars = new char[n];
         int pos = 0;

         while (pos < n) {
             chars[pos] = next();
             if (end()) {
                 throw syntaxError("Substring bounds error");                 
             }
             pos += 1;
         }
         return new String(chars);
     }


    /**
     * Get the next char in the string, skipping whitespace.
     * @throws JSONException
     * @return  A character, or 0 if there are no more characters.
     */
    public char nextClean() throws JSONException {
        for (;;) {
            char c = next();
            if (c == 0 || c > ' ') {
                return c;
            }
        }
    }


    /**
     * Return the characters up to the next close quote character.
     * Backslash processing is done. The formal JSON format does not
     * allow strings in single quotes, but an implementation is allowed to
     * accept them.
     * @param quote The quoting character, either
     *      <code>"</code>&nbsp;<small>(double quote)</small> or
     *      <code>'</code>&nbsp;<small>(single quote)</small>.
     * @return      A String.
     * @throws JSONException Unterminated string.
     */
    public String nextString(char quote) throws JSONException {
        char c;
        StringBuffer sb = new StringBuffer();
        for (;;) {
            c = next();
            switch (c) {
            case 0:
            case '\n':
            case '\r':
                throw syntaxError("Unterminated string");
            case '\\':
                c = next();
                switch (c) {
                case 'b':
                    sb.append('\b');
                    break;
                case 't':
                    sb.append('\t');
                    break;
                case 'n':
                    sb.append('\n');
                    break;
                case 'f':
                    sb.append('\f');
                    break;
                case 'r':
                    sb.append('\r');
                    break;
                case 'u':
                    sb.append((char)Integer.parseInt(next(4), 16));
                    break;
                case '"':
                case '\'':
                case '\\':
                case '/':
                	sb.append(c);
                	break;
                default:
                    throw syntaxError("Illegal escape.");
                }
                break;
            default:
                if (c == quote) {
                    return sb.toString();
                }
                sb.append(c);
            }
        }
    }


    /**
     * Get the text up but not including the specified character or the
     * end of line, whichever comes first.
     * @param  delimiter A delimiter character.
     * @return   A string.
     */
    public String nextTo(char delimiter) throws JSONException {
        StringBuffer sb = new StringBuffer();
        for (;;) {
            char c = next();
            if (c == delimiter || c == 0 || c == '\n' || c == '\r') {
                if (c != 0) {
                    back();
                }
                return sb.toString().trim();
            }
            sb.append(c);
        }
    }


    /**
     * Get the text up but not including one of the specified delimiter
     * characters or the end of line, whichever comes first.
     * @param delimiters A set of delimiter characters.
     * @return A string, trimmed.
     */
    public String nextTo(String delimiters) throws JSONException {
        char c;
        StringBuffer sb = new StringBuffer();
        for (;;) {
            c = next();
            if (delimiters.indexOf(c) >= 0 || c == 0 ||
                    c == '\n' || c == '\r') {
                if (c != 0) {
                    back();
                }
                return sb.toString().trim();
            }
            sb.append(c);
        }
    }


    /**
     * Get the next value. The value can be a Boolean, Double, Integer,
     * JSONArray, JSONObject, Long, or String, or the JSONObject.NULL object.
     * @throws JSONException If syntax error.
     *
     * @return An object.
     */
    public Object nextValue() throws JSONException {
        char c = nextClean();
        String string;

        switch (c) {
            case '"':
            case '\'':
                return nextString(c);
            case '{':
                back();
                return new JSONObject(this);
            case '[':
                back();
                return new JSONArray(this);
        }

        /*
         * Handle unquoted text. This could be the values true, false, or
         * null, or it can be a number. An implementation (such as this one)
         * is allowed to also accept non-standard forms.
         *
         * Accumulate characters until we reach the end of the text or a
         * formatting character.
         */

        StringBuffer sb = new StringBuffer();
        while (c >= ' ' && ",:]}/\\\"[{;=#".indexOf(c) < 0) {
            sb.append(c);
            c = next();
        }
        back();

        string = sb.toString().trim();
        if (string.equals("")) {
            throw syntaxError("Missing value");
        }
        return JSONObject.stringToValue(string);
    }


    /**
     * Skip characters until the next character is the requested character.
     * If the requested character is not found, no characters are skipped.
     * @param to A character to skip to.
     * @return The requested character, or zero if the requested character
     * is not found.
     */
    public char skipTo(char to) throws JSONException {
        char c;
        try {
            int startIndex = this.index;
            int startCharacter = this.character;
            int startLine = this.line;
            reader.mark(Integer.MAX_VALUE);
            do {
                c = next();
                if (c == 0) {
                    reader.reset();
                    this.index = startIndex;
                    this.character = startCharacter;
                    this.line = startLine;
                    return c;
                }
            } while (c != to);
        } catch (IOException exc) {
            throw new JSONException(exc);
        }

        back();
        return c;
    }
    

    /**
     * Make a JSONException to signal a syntax error.
     *
     * @param message The error message.
     * @return  A JSONException object, suitable for throwing
     */
    public JSONException syntaxError(String message) {
        return new JSONException(message + toString());
    }


    /**
     * Make a printable string of this JSONTokener.
     *
     * @return " at {index} [character {character} line {line}]"
     */
    public String toString() {
        return " at " + index + " [character " + this.character + " line " + 
        	this.line + "]";
    }
}

class CloneUtil {
	
	/**
	 * Returns clone of given object only if it implements Serialization
	 * @param object - serializable object
	 * @return new copy of passed object
	 */
    public static Object clone(Object serializableObject) {
		try {
			// Step 1 : Serialize Object
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(baos);
			objectOutputStream.writeObject(serializableObject);
			
			// Step 2 : Deserialize Object
			ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
			return objectInputStream.readObject();
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
    }
}

enum Direction {
	INCOMING,
	OUTGOING,
	BOTH;

	public Direction reverse() {
		if(this.compareTo(Direction.INCOMING) == 0) {
			return Direction.OUTGOING;
		} else if(this.compareTo(Direction.OUTGOING) == 0) {
			return Direction.INCOMING;
		} else {
			return Direction.BOTH;
		}
	}
   public static Direction getType(String name)
    {
        for(Direction direction : Direction.values())
        {
            if(direction.toString().equalsIgnoreCase(name)) { 
                return direction;
            }
        }
        return null;
    }
   
    public String toString() {
     	switch(this) {
	    	case INCOMING		: return "incoming";
	    	case OUTGOING		: return "outgoing";
	    	case BOTH			: return "both";
	    	default 			: return this.name();
    	}
    }
}

interface NodeSet {
	public Set<Node> getNodeSet();
	
	public List<Node> getNodeList();
	
	public List<String> getNodeIdList();
	
	public Node getNodeById(String nodeId);
	
	public Node getNodeByName(String nodeName);
	
	public void addNode(Node node);
	
	public void addAllNode(List<Node> nodeList);
	
	public void setNodeSet(Set<Node> nodeSet);
	
	public JSONArray toJSONArray() throws JSONException;
	
	public String toString();
	
	public Map<String,Node> getNodeNameNodeMap();
	
	public Map<String,String> getNodeNameNodeIdMap();
	
	public Map<String,String> getNodeNameNodeIdMapLowercaseNodeName();
	
	public Map<String,Node> getNodeNameNodeMapLowercaseNodeName();
	
	public Map<String,Node> getNodeIdNodeMap();

	List<String> getNodeNameList();
}

class NodeSetImpl implements NodeSet, Serializable {
	private static final long serialVersionUID = 1L;
	
	private Set<Node> nodeSet;
	
	public NodeSetImpl() {
	}

	public NodeSetImpl(Set<Node> nodeSet) {
		this.nodeSet = nodeSet;
	}

	public NodeSetImpl(Node node)
	{
		this.nodeSet = new LinkedHashSet<Node>();
		this.nodeSet.add(node);
	}

	public NodeSetImpl(List<Node> nodeList) {
		this.nodeSet = new LinkedHashSet<Node>(nodeList);
	}

	public NodeSetImpl(String multipleNodeArrayJSON) throws JSONException {
		JSONArray jsObjectNodes = new JSONArray(multipleNodeArrayJSON);
		Set<Node> nodeSet = new LinkedHashSet<Node>();
		for(int i=0; i<jsObjectNodes.length(); i++) 
		{
			JSONObject jsObjectNode = jsObjectNodes.getJSONObject(i);
			nodeSet.add(new NodeImpl(jsObjectNode.toString()));
		}
		setNodeSet(nodeSet);
	}
	
	public Set<Node> getNodeSet() {
		return nodeSet;
	}

	public List<Node> getNodeList() {
		if(this.nodeSet == null || this.nodeSet.isEmpty()) {
			return null;
		}
		return new ArrayList<Node>(nodeSet);
	}
	
	public List<String> getNodeIdList() {
		if(this.nodeSet == null || this.nodeSet.isEmpty()) {
			return null;
		}
		
		List<String> nodeIdList = new ArrayList<String>();
		
		for(Node node : this.nodeSet) {
			nodeIdList.add(node.getId());
		}
		
		return nodeIdList;
	}
	
	@Override
	public List<String> getNodeNameList() {
		if(this.nodeSet == null || this.nodeSet.isEmpty()) {
			return null;
		}
		
		List<String> nodeNameList = new ArrayList<String>();
		
		for(Node node : this.nodeSet) {
			nodeNameList.add(node.getName());
		}
		
		return nodeNameList;
	}
	
	public Node getNodeById(String nodeId) {
		if(this.nodeSet == null || this.nodeSet.isEmpty()) {
			return null;
		}
		
		for(Node node : getNodeList()) {
			if(node.getId().equals(nodeId)) {
				return node;
			}
		}
		
		return null;
	}
	
	public Node getNodeByName(String nodeName) {
		if(this.nodeSet == null || this.nodeSet.isEmpty()) {
			return null;
		}
		
		for(Node node : getNodeList()) {
			if(node.getName().equals(nodeName)) {
				return node;
			}
		}
		
		return null;
	}

	public void setNodeSet(Set<Node> nodeSet) {
		this.nodeSet = nodeSet;
	}

	public void addNode(Node node)
	{
		if(this.nodeSet == null) {
			this.nodeSet = new LinkedHashSet<Node>();
		}
		this.nodeSet.add(node);
	}
	
	public void addAllNode(List<Node> nodeList)
	{
		if(this.nodeSet == null) {
			this.nodeSet = new LinkedHashSet<Node>();
		}
		this.nodeSet.addAll(nodeList);
	}

	public String toString() {
		try {
			return toJSONString();
		} catch (JSONException e) {
			return "";
		}
	}

	public NodeSet clone() {
		return (NodeSet)CloneUtil.clone(this);
	}
	
	public String toJSONString() throws JSONException {
		return toJSONArray().toString();
	}
	
	public JSONArray toJSONArray() throws JSONException 
	{
		JSONArray nodeArray = new JSONArray();
		for(Node node : getNodeSet()) {
			nodeArray.put(node.toJSONObject());
		}
		return nodeArray;
	}
	
	public Map<String,String> getNodeNameNodeIdMap(){
		Map<String,String> contextMap = new HashMap<String,String>();
		if(this.getNodeList() == null){
			return new HashMap<String, String>();
		}
		for(Node node : this.getNodeList()) {
			String nodeName = node.getName();
			String nodeId = node.getId();

			contextMap.put(nodeName, nodeId);
		}
		return contextMap;
	}
	
	public Map<String,Node> getNodeIdNodeMap(){
		Map<String,Node> contextMap = new HashMap<String,Node>();
		if(this.getNodeList() == null){
			return new HashMap<String, Node>();
		}
		for(Node node : this.getNodeList()) {
			String nodeId = node.getId();

			contextMap.put(nodeId, node);
		}
		return contextMap;
	}

	public Map<String,String> getNodeNameNodeIdMapLowercaseNodeName(){
		Map<String,String> contextMap = new HashMap<String,String>();
		if(this.getNodeList() == null){
			return new HashMap<String, String>();
		}
		for(Node node : this.getNodeList()) {
			String nodeName = node.getName();
			String nodeId = node.getId();

			contextMap.put(nodeName.toLowerCase(), nodeId);
		}
		return contextMap;
	}
	
	public Map<String,Node> getNodeNameNodeMap(){
		Map<String,Node> contextMap = new HashMap<String,Node>();
		if(this.getNodeList() == null){
			return new HashMap<String, Node>();
		}
		
		for(Node node : this.getNodeList()) {
			String nodeName = node.getName();
			//String nodeId = node.getId();
			contextMap.put(nodeName, node);
		}
		return contextMap;
	}
	
	public Map<String,Node> getNodeNameNodeMapLowercaseNodeName(){
		Map<String,Node> contextMap = new HashMap<String,Node>();
		if(this.getNodeList() == null){
			return new HashMap<String, Node>();
		}
		
		for(Node node : this.getNodeList()) {
			String nodeName = node.getName();
			//String nodeId = node.getId();
			contextMap.put(nodeName.toLowerCase(), node);
		}
		return contextMap;
	}
}

interface Relation {
	public String getId();
	public void setId(String id);
	
	public RelationType getRelationType();
	public void setRelationType(RelationType relationType);
	public void setRelationTypeId(String relationTypeId);
	
	public void setRelationTypeName(String relationTypeName);
	
	public Node getStartNode();
	public void setStartNode(Node startNode);
	public void setStartNodeId(String startNodeId);
	
	public Node getEndNode();
	public void setEndNode(Node endNode);
	public void setEndNodeId(String endNodeId);

	public Node getOtherNode(Node node);
	public Node getOtherNode(String nodeId);
	
	public String getProperty(String key);
	public void setProperty(String key, String value);
	public Set<String> getPropertyKeySet();
	
	public Map<String, String> getPropertyMap();
	public void setPropertyMap(Map<String, String> propertyMap);

	public void removeProperty(String key);

	public String toFormattedString();
	
	public JSONObject toJSONObject() throws JSONException;
	public String toJSONString() throws JSONException;
}

interface RelationType {
	public String getId();
	public void setId(String id);
	
	public String getName();
	public void setName(String name);
	
	public String toJSONString() throws JSONException;
	public JSONObject toJSONObject() throws JSONException;
}

class RelationTypeImpl implements RelationType, Serializable {
	private static final long serialVersionUID = 1L;
	
	private String id;
	private String name;
	
	public RelationTypeImpl() {
	}

	public RelationTypeImpl(String jsonString) throws JSONException {
		
		JSONObject newJSONRelationType = new JSONObject(jsonString);
		
		setId(newJSONRelationType.optString("relationTypeId"));
		setName(newJSONRelationType.optString("relationTypeName"));
	}
	
	public RelationTypeImpl(String id, String name) {
		this.id = id;
		this.name = name;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public boolean equals(Object obj)
	{
		if(this == obj) {
			return true;
		}
		if((obj == null) || (obj.getClass() != this.getClass())) {
			return false;
		}
		// Object must be CalendarTimeUnit at this point
		RelationType anotherRelationType = (RelationType)obj;
		if(this.id == null || anotherRelationType.getId() == null) {
			return false;
		}
		
		if(this.name == null || anotherRelationType.getName() == null) {
			return false;
		}
		
		return (this.name.equals(anotherRelationType.getName()) && 
				this.id.equals(anotherRelationType.getId()));
	}

	public int hashCode()
	{
		int hash = 7;
		hash = 31 * hash + this.id.hashCode();
		hash = 31 * hash + this.name.hashCode();
		return hash;
	}
	
	public JSONObject toJSONObject() throws JSONException {
		JSONObject entity = new JSONObject();
		entity.put("id", id);
		entity.put("name", name);
		return entity;
	}
	
	public String toJSONString() throws JSONException {
		return toJSONObject().toString();
	}
	
	public static <T extends RelationType> JSONArray toJSONArray(List<? extends RelationType> tList) throws JSONException {
		JSONArray jsonArray = new JSONArray();
		if(tList != null) {
			for(RelationType entity : tList) {
				jsonArray.put(entity.toJSONObject());
			}
		}
		return jsonArray;
	}

	public static <T extends RelationType> String toJSONArrayString(List<? extends RelationType> tList) throws JSONException {
		return toJSONArray(tList).toString();
	}

	public String toString() {
		try {
			return toJSONString();
		} catch (JSONException e) {
			return "";
		}
	}
}

class FileDataSet {

	String filePath;
	String sheetName;
	int sheetNumber;
	List<String> headerNameList;
	Map<String,String> firstRowMap;
	List<Map<String,String>> dataMapList;
	
	FileDataSet(){
	}

	public FileDataSet(String filePath){
		this.filePath = filePath;
	}

	public FileDataSet(String filePath, String sheetName){
		this.filePath = filePath;
		this.sheetName = sheetName;
	}

	public FileDataSet(String filePath, int sheetNumber){
		this.filePath = filePath;
		this.sheetNumber = sheetNumber;
	}

	public FileDataSet(String filePath, List<String> headerNameList,Map<String, String> firstRowMap, List<Map<String, String>> dataMap) {
		this.filePath = filePath;
		this.headerNameList = headerNameList;
		this.firstRowMap = firstRowMap;
		this.dataMapList = dataMap;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public String getSheetName() {
		return sheetName;
	}

	public void setSheetName(String sheetName) {
		this.sheetName = sheetName;
	}

	public int getSheetNumber() {
		return sheetNumber;
	}

	public void setSheetNumber(int sheetNumber) {
		this.sheetNumber = sheetNumber;
	}

	public List<String> getHeaderNameList() {
		return headerNameList;
	}

	public void setHeaderNameList(List<String> headerNameList) {
		this.headerNameList = headerNameList;
	}

	public Map<String, String> getFirstRowMap() {
		return firstRowMap;
	}

	public void setFirstRowMap(Map<String, String> firstRowMap) {
		this.firstRowMap = firstRowMap;
	}

	public List<Map<String, String>> getDataMapList() {
		return dataMapList;
	}

	public void setDataMapList(List<Map<String, String>> dataMapList) {
		this.dataMapList = dataMapList;
	}
}

interface FileReaderHelper {
	
	public enum FileType{
		XLS,XLSX,XLSB,XLSM,CSV,ZIP;
		 public static FileType getType(String type)
	    {
	        for(FileType fileType : FileType.values())
	        {
	            if(fileType.toString().equalsIgnoreCase(type)) { 
	                return fileType;
	            }
	        }
	        return null;
	    }
		 
		public String toString() 
	    {
	    	switch(this) {
	    		case XLS  : return "xls";
	    		case XLSX : return "xlsx";
	    		case XLSB : return "xlsb";
	    		case XLSM : return "xlsm";
	    		case CSV  : return "csv";
	    		case ZIP  : return "zip";
		    	default   : return this.toString();
		    }
	    }
	}
	
	public static final String DUMMY_SHEET_NAME_FOR_CSV_FILE = "Sheet";
	public static final String PIVOT_DUMP_SHEET = "Data Dump_Automated";
	
	public Map<String, List<Map<String, String>>> readAll() throws  Exception;
	public Map<String, String> readFirstRow() throws  Exception;
	public List<String> readHeaderRow() throws  Exception;
	public List<String> getSheetList() throws Exception;
	public String getOrientation();
	public Map<Integer,List<String>> getRowIdToRowDataFromSourceFile(FileReaderMemoryOptimized.DataReadType dataReadType) throws Exception;
	public void setConcatenateIndex(boolean concatenateIndex);
	public void setStartRowNo(int startRowNo);
	public void setSheetIndex(int sheetIndex);
	public void setSheetName(String sheetName);
	public void setHeaderRowNumberList(List<Integer> headerRowNumberList);
	public void setOrientation(String orientation);
	public void setTagNodeTypeMap(Map<String,String> tagNodeTypeMap);
	
	public void setIncludeColumnsIndexList(Set<Integer> headerIndexList);
	
	public Map<String, List<Map<String, String>>> readDataFromDocFile() throws  Exception;
	public List<String> readHeaderRowFromDocFile() throws  Exception;
	public Map<String, String> readFirstRowFromDocFile() throws  Exception;
	public Map<String, List<Map<String, String>>> getRowDataMapFromDocFile(FileReaderMemoryOptimized.DataReadType dataReadType) throws Exception;
	public Map<Integer, List<String>> readAllNew() throws Exception;
	public List<String> readHeaderRowNew() throws Exception;
	public Map<Integer, List<String>> checkColumnOrientation( int rowMaxForColOrientation, int colMaxForColOrientation, int nextStartRow, int nextStartCol) throws Exception;
	public Map<Integer, List<String>> checkRowOrientation(int rowMaxForColOrientation, int colMaxForColOrientation, int nextStartRow, int nextStartCol) throws Exception;
}

class FileReaderMemoryOptimized implements FileReaderHelper {

	
	private static final String HEADER_INDEX_DELIMITER = "_"; 

	/**
	 * Default values
	 */
	private boolean concatenateIndex = false;
	private int startRowNo = 0;
	private int sheetIndex = 0;
	private final File sourceFile;
	private InputStream inputStream;
	private String urlPath;
	private String sheetName;
	private Map<Integer,List<String>> rowIdToRowDataFromSourceFile_Cache;
	private Set<Integer> includeColumnsIndexList;
	private List<Integer> headerRowNumberList = new ArrayList<Integer>();
	private String orientation = null;
	private Map<String,String> tagNodeTypeMap;
	public static enum DataReadType{
		ALL,HEADER,FIRSTROW;
	}
	
	public FileReaderMemoryOptimized(String readFile){
		this.sourceFile = new File(readFile);
	}
	
	public FileReaderMemoryOptimized(String readFile,boolean concatenateIndex){
		this(readFile);
		this.concatenateIndex = concatenateIndex;
	}
	public FileReaderMemoryOptimized(String readFile,boolean concatenateIndex, int startRowNo){
		this(readFile, concatenateIndex);
		this.startRowNo = startRowNo;
	}
	public FileReaderMemoryOptimized(String readFile,boolean concatenateIndex, int startRowNo, int sheetIndex){
		this(readFile, concatenateIndex, startRowNo);
		this.sheetIndex = sheetIndex;
	}
	public FileReaderMemoryOptimized(String readFile,boolean concatenateIndex, int startRowNo, String sheetName){
		this(readFile, concatenateIndex, startRowNo);
		this.sheetName = sheetName;
	}
	public FileReaderMemoryOptimized(String readFile,boolean concatenateIndex, List<Integer> headerRowNumberList){
		this(readFile, concatenateIndex);
		this.headerRowNumberList = headerRowNumberList;
		if(headerRowNumberList!=null && !headerRowNumberList.isEmpty()){
			Collections.sort(headerRowNumberList);
			startRowNo = headerRowNumberList.get(headerRowNumberList.size()-1);
		}
	}

	public FileReaderMemoryOptimized(InputStream inputStream, String urlPath){
		this.inputStream = inputStream;
		this.urlPath = urlPath;
		this.sourceFile = null;
	}
	
	public FileReaderMemoryOptimized(InputStream inputStream,boolean concatenateIndex, String urlPath){
		this(inputStream,urlPath);
		this.concatenateIndex = concatenateIndex;
	}
	public FileReaderMemoryOptimized(InputStream inputStream,boolean concatenateIndex, int startRowNo, String urlPath){
		this(inputStream, concatenateIndex,urlPath);
		this.startRowNo = startRowNo;
	}
	public FileReaderMemoryOptimized(InputStream inputStream,boolean concatenateIndex, int startRowNo, int sheetIndex, String urlPath){
		this(inputStream, concatenateIndex, startRowNo,urlPath);
		this.sheetIndex = sheetIndex;
	}
	public FileReaderMemoryOptimized(InputStream inputStream,boolean concatenateIndex, int startRowNo, String sheetName, String urlPath){
		this(inputStream, concatenateIndex, startRowNo,urlPath);
		this.sheetName = sheetName;
	}
	public FileReaderMemoryOptimized(InputStream inputStream,boolean concatenateIndex, List<Integer> headerRowNumberList, String urlPath){
		this(inputStream, concatenateIndex,urlPath);
		this.headerRowNumberList = headerRowNumberList;
		if(headerRowNumberList!=null && !headerRowNumberList.isEmpty()){
			Collections.sort(headerRowNumberList);
			startRowNo = headerRowNumberList.get(headerRowNumberList.size()-1);
		}
	}

	public Set<Integer> getIncludeColumnsIndexList() {
		return includeColumnsIndexList;
	}

	/**
	 * Not to use this List for filtering columns, because in case of datasource read, header index gets mismatched
	 * @param headerIndexList
	 */
	public void setIncludeColumnsIndexList(Set<Integer> headerIndexList) {
		this.includeColumnsIndexList = headerIndexList;
	}

	public boolean isConcatenateIndex() {
		return concatenateIndex;
	}
	public void setConcatenateIndex(boolean concatenateIndex) {
		this.concatenateIndex = concatenateIndex;
	}
	public int getStartRowNo() {
		return startRowNo;
	}
	public void setStartRowNo(int startRowNo) {
		this.startRowNo = startRowNo;
	}
	public int getSheetIndex() {
		return sheetIndex;
	}
	public void setSheetIndex(int sheetIndex) {
		this.sheetIndex = sheetIndex;
	}
	public String getSheetName() {
		return sheetName;
	}
	public void setSheetName(String sheetName) {
		this.sheetName = sheetName;
	}
	public List<Integer> getHeaderRowNumberList() {
		return headerRowNumberList;
	}
	public void setHeaderRowNumberList(List<Integer> headerRowNumberList) {
		this.headerRowNumberList = headerRowNumberList;
	}
	
	private FileType getFileType() throws Exception{
		return getFileType(sourceFile!=null?sourceFile.getPath():urlPath);
	}

	public String getOrientation() {
		return orientation;
	}
	
	public void setOrientation(String orientation) {
		this.orientation = orientation;
	}

	public void setTagNodeTypeMap(Map<String,String> tagNodeTypeMap) {
		this.tagNodeTypeMap = tagNodeTypeMap;		
	}
	
	public static FileType getFileType(String path) throws Exception{
		String lowerCaseFilePath = path.toLowerCase();
		String extension = lowerCaseFilePath.substring(lowerCaseFilePath.lastIndexOf(".")+1, lowerCaseFilePath.length());
		if(FileType.getType(extension) == null){
			throw new Exception("File Type not found for this file :"+path);	
		}
		return FileType.getType(extension);
	} 

	@Override
	public Map<String, List<Map<String, String>>> readAll() throws Exception {
		if(sourceFile!=null){
			if (!this.sourceFile.exists()) {
				throw new Exception(sourceFile.getPath()+": File not found..");
			}			
		}

		if(sourceFile!=null){
			System.out.println(sourceFile.getPath()+": Reading started");			
		}else{
			System.out.println(urlPath+": Reading started");			
		}
		Map<String, List<Map<String, String>>> returnSheetWiseRowDataList = new HashMap<String, List<Map<String, String>>>();
		Map<Integer,List<String>> rowIdToRowData = null;
		List<Map<String, String>> rowDataList = new ArrayList<Map<String, String>>();

		try {
			rowIdToRowData = getRowIdToRowDataFromSourceFile(DataReadType.ALL);
//			List<String> headerList = rowIdToRowData.get(startRowNo);
			List<String> headerList = readHeaderRow();
			for(Map.Entry<Integer,List<String>> entry : rowIdToRowData.entrySet()){
				int rowIndex = entry.getKey();
				if(rowIndex > startRowNo ){
					Map<String,String> rowData = new HashMap<String,String>();
					int colIndex = 0;
					for(String columnValue: entry.getValue()){
						if(colIndex < headerList.size()){
							rowData.put(headerList.get(colIndex).trim(), columnValue.trim());
							colIndex++;
						}
					}
					rowDataList.add(rowData);
				}
			}
			if(this.sheetName == null){
				returnSheetWiseRowDataList.put(String.valueOf(sheetIndex),rowDataList);	
			}else{
				returnSheetWiseRowDataList.put(this.sheetName,rowDataList);
			}

		} catch (Exception e) {
			throw new Exception(e);
		} finally{
			rowIdToRowData = null;
			rowDataList = null;
		}

		if(sourceFile!=null){
			System.out.println(sourceFile.getPath()+": Reading ended");			
		}else{
			System.out.println(urlPath+": Reading ended");			
		}
		return returnSheetWiseRowDataList;
	
	}

	@Override
	public Map<Integer, List<String>> checkColumnOrientation(int rowMaxForColOrientation, int colMaxForColOrientation, int nextStartRow,  int nextStartCol) throws Exception {
		if(sourceFile!=null){
			if (!this.sourceFile.exists()) {
				throw new Exception(sourceFile.getPath()+": File not found..");
			}			
		}

		Map<Integer,List<String>> outputMap = null;
		try {
			outputMap = getFileOrientationByNodeType(DataReadType.ALL,rowMaxForColOrientation,colMaxForColOrientation,nextStartRow,nextStartCol,false);			
		} catch (Exception e) {
			throw new Exception(e);
		}
		
		return outputMap;	
	}

	@Override
	public Map<Integer, List<String>> checkRowOrientation(int rowMaxForColOrientation, int colMaxForColOrientation,int nextStartRow, int nextStartCol) throws Exception {
		if(sourceFile!=null){
			if (!this.sourceFile.exists()) {
				throw new Exception(sourceFile.getPath()+": File not found..");
			}			
		}

		Map<Integer,List<String>> outputMap = null;
		try {
			outputMap = getFileOrientationByNodeType(DataReadType.ALL,rowMaxForColOrientation,colMaxForColOrientation,nextStartRow,nextStartCol,true);			
		} catch (Exception e) {
			throw new Exception(e);
		}
		
		return outputMap;	
	}

	@Override
	public Map<Integer, List<String>> readAllNew() throws Exception {
		if(sourceFile!=null){
			System.out.println(sourceFile.getPath()+": Reading started");			
		}else{
			System.out.println(urlPath+": Reading started");			
		}
		
		Map<Integer, List<String>> outputMap = new HashMap<Integer, List<String>>();
		try {
			outputMap = getRowIdToRowDataFromSourceFileWithOrientation(DataReadType.ALL);
		} catch (Exception e) {
			throw new Exception(e);
		}

		if(sourceFile!=null){
			System.out.println(sourceFile.getPath()+": Reading ended");			
		}else{
			System.out.println(urlPath+": Reading ended");			
		}
		return outputMap;
	
	}

	@Override
	public Map<String, String> readFirstRow() throws Exception {
		if(sourceFile!=null){
			if (!this.sourceFile.exists()) {
				throw new Exception(sourceFile.getPath()+": File not found..");
			}			
		}

		Map<String,String> rowData = new HashMap<String,String>();
		try {
			Map<Integer,List<String>> rowIdToRowData = getRowIdToRowDataFromSourceFile(DataReadType.FIRSTROW);
			List<String> headerList =  readHeaderRow();
			int firstDataRowIndex = startRowNo + 1;
			if(!rowIdToRowData.containsKey(firstDataRowIndex)){
				return rowData;
			}
			int colIndex = 0;
			for(String columnValue: rowIdToRowData.get(firstDataRowIndex) ){
				if(colIndex < headerList.size()){
					rowData.put(headerList.get(colIndex).trim(), columnValue.trim());
					colIndex++;
				}
			}
		} catch (Exception e) {
			throw new Exception(e);
		}
		return rowData;
	}


	@Override
	public List<String> readHeaderRow() throws Exception {

		List<String> headerList = new ArrayList<String>();
		try {
			if(headerRowNumberList!=null && !headerRowNumberList.isEmpty()){
				List<String> modifiedHeaderList = new ArrayList<String>();
				for(int headerRowNumber : headerRowNumberList){
					startRowNo = headerRowNumber;
					rowIdToRowDataFromSourceFile_Cache = null;
					Map<Integer,List<String>> rowIdToRowData = getRowIdToRowDataFromSourceFile(DataReadType.HEADER);
					List<String> rowheaderList = rowIdToRowData.get(startRowNo);
					List<String> modifiedRowHeaderList = new ArrayList<String>();
					if(rowheaderList!=null){
						int i=0;
						for(i=0;i<rowheaderList.size();i++){
							if(i<modifiedHeaderList.size()){
								if(modifiedHeaderList.get(i).trim().equals("")){
									modifiedRowHeaderList.add(rowheaderList.get(i).trim());									
								}else{
									if(rowheaderList.get(i).trim().equals("")){
										modifiedRowHeaderList.add(modifiedHeaderList.get(i).trim());									
									}else{
										modifiedRowHeaderList.add(modifiedHeaderList.get(i).trim()+"_"+rowheaderList.get(i).trim());																			
									}
								}
							}else{
								modifiedRowHeaderList.add(rowheaderList.get(i).trim());								
							}
						}
						while(i<modifiedHeaderList.size()){
							modifiedRowHeaderList.add(modifiedHeaderList.get(i).trim());
							i++;
						}
					}
					modifiedHeaderList = modifiedRowHeaderList;
				}	
				if(concatenateIndex){
					int colIndex = 0;
					for(String header : modifiedHeaderList){
						headerList.add(header.trim().concat(HEADER_INDEX_DELIMITER).concat(String.valueOf(colIndex)));
						colIndex++;
					}
				}else{
					headerList = modifiedHeaderList;					
				}
			}else{
				Map<Integer,List<String>> rowIdToRowData = getRowIdToRowDataFromSourceFile(DataReadType.HEADER);
				if(concatenateIndex){
					int colIndex = 0;
					for(String header : rowIdToRowData.get(startRowNo)){
						headerList.add(header.trim().concat(HEADER_INDEX_DELIMITER).concat(String.valueOf(colIndex)));
						colIndex++;
					}
				}else{
					headerList = rowIdToRowData.get(startRowNo);
				}				
			}
		}catch (Exception e) {
			throw new Exception(e);
		} 
		return headerList;

	}

	@Override
	public List<String> readHeaderRowNew() throws Exception {

		List<String> headerList = new ArrayList<String>();
		try {
			if(headerRowNumberList!=null && !headerRowNumberList.isEmpty()){
				List<String> modifiedHeaderList = new ArrayList<String>();
				for(int headerRowNumber : headerRowNumberList){
					startRowNo = headerRowNumber;
					rowIdToRowDataFromSourceFile_Cache = null;
					Map<Integer,List<String>> rowIdToRowData = getRowIdToRowDataFromSourceFileWithOrientation(DataReadType.HEADER);
					List<String> rowheaderList = rowIdToRowData.get(startRowNo);
					List<String> modifiedRowHeaderList = new ArrayList<String>();
					if(rowheaderList!=null){
						int i=0;
						for(i=0;i<rowheaderList.size();i++){
							if(i<modifiedHeaderList.size()){
								if(modifiedHeaderList.get(i).trim().equals("")){
									modifiedRowHeaderList.add(rowheaderList.get(i).trim());									
								}else{
									if(rowheaderList.get(i).trim().equals("")){
										modifiedRowHeaderList.add(modifiedHeaderList.get(i).trim());									
									}else{
										modifiedRowHeaderList.add(modifiedHeaderList.get(i).trim()+"_"+rowheaderList.get(i).trim());																			
									}
								}
							}else{
								modifiedRowHeaderList.add(rowheaderList.get(i).trim());								
							}
						}
						while(i<modifiedHeaderList.size()){
							modifiedRowHeaderList.add(modifiedHeaderList.get(i).trim());
							i++;
						}
					}
					modifiedHeaderList = modifiedRowHeaderList;
				}	
				if(concatenateIndex){
					int colIndex = 0;
					for(String header : modifiedHeaderList){
						headerList.add(header.trim().concat(HEADER_INDEX_DELIMITER).concat(String.valueOf(colIndex)));
						colIndex++;
					}
				}else{
					headerList = modifiedHeaderList;					
				}
			}else{
				Map<Integer,List<String>> rowIdToRowData = getRowIdToRowDataFromSourceFile(DataReadType.HEADER);
				if(concatenateIndex){
					int colIndex = 0;
					for(String header : rowIdToRowData.get(startRowNo)){
						headerList.add(header.trim().concat(HEADER_INDEX_DELIMITER).concat(String.valueOf(colIndex)));
						colIndex++;
					}
				}else{
					headerList = rowIdToRowData.get(startRowNo);
				}				
			}
		}catch (Exception e) {
			throw new Exception(e);
		} 
		return headerList;

	}

	public List<String> getSheetList() throws Exception {
		FileType fileType = getFileType();
		if(fileType == FileType.XLSX || fileType == FileType.XLSM){
			OPCPackage p = null;
			try {
				if(sourceFile==null){
					p = OPCPackage.open(inputStream);				
				}else{
					p = OPCPackage.open(sourceFile.getPath(), PackageAccess.READ);				
				}
				XSSFReader xssfReader = new XSSFReader(p);
				return FileReaderXLSXMRowExtractor.getTotalSheetList(xssfReader);
			} catch (Exception e1) {
				e1.printStackTrace();
				throw new Exception();
			}finally{
				if(p != null){
					p.close();
				}
			}
		}else if(fileType == FileType.XLS){
			POIFSFileSystem POIFS = new POIFSFileSystem(sourceFile);
			try{
				FileReaderXLSRowExtractor extractRowDataFromXExcelFile = new FileReaderXLSRowExtractor(POIFS);
				return extractRowDataFromXExcelFile.getTotalSheetList();
			}finally{
				POIFS.close();
			}
		}else{
			List<String> sheetList = new ArrayList<String>();
			String sheetName = "Dummy";
			if(sourceFile!=null){
				String filePath = sourceFile.getAbsolutePath();
				sheetName = filePath.substring(filePath.lastIndexOf("\\")+1,filePath.lastIndexOf("."));
			}else{
				sheetName = urlPath.substring(urlPath.lastIndexOf("\\")+1,urlPath.lastIndexOf("."));
			}
			sheetList.add(sheetName);
			return sheetList;
//			throw new FileTypeNotSupportedException("Sheets not supported for this file type:"+sourceFile);
		}
	}

	public Map<Integer,List<String>> getFileOrientationByNodeType(DataReadType dataReadType, int rowMax, int colMax, int nextStartRow, int nextStartCol, boolean isCheckRowOrientation) throws Exception{
		Map<Integer,List<String>> outputMap = new HashMap<Integer,List<String>>();
		FileType fileType = getFileType();
//		Map<Integer,List<String>> rowIdToRowData = new HashMap<Integer,List<String>>();
		if(fileType == FileType.XLSX || fileType == FileType.XLSM){
			OPCPackage p;
			if(sourceFile==null){
				p = OPCPackage.open(inputStream);				
			}else{
				p = OPCPackage.open(sourceFile.getPath(), PackageAccess.READ);				
			}
			try{
				FileReaderXLSXMRowExtractor extractRowDataFromExcelFile = new FileReaderXLSXMRowExtractor(outputMap, this.includeColumnsIndexList,dataReadType,nextStartRow,true,isCheckRowOrientation,null,rowMax,colMax,nextStartCol);
				InputStream sheetInputStream = null;
				ReadOnlySharedStringsTable strings = new ReadOnlySharedStringsTable(p);
				XSSFReader xssfReader = new XSSFReader(p);
				StylesTable styles = xssfReader.getStylesTable();
				if(this.sheetName == null){
					sheetInputStream = extractRowDataFromExcelFile.getSheetInputStreamBySheetIndex(xssfReader,sheetIndex);
				}else{
					sheetInputStream = extractRowDataFromExcelFile.getSheetInputStreamBySheetName(xssfReader,sheetName);
				}
				outputMap = extractRowDataFromExcelFile.checkDataOrientation(styles, strings,extractRowDataFromExcelFile.new CustomSheetContentHandler(), sheetInputStream);
			}finally{
				p.close();
			}

		}else if(fileType == FileType.XLS){
			POIFSFileSystem POIFS = new POIFSFileSystem(sourceFile);
			try{
				FileReaderXLSRowExtractor extractRowDataFromXExcelFile = null;
				if(this.sheetName == null){
					extractRowDataFromXExcelFile = new FileReaderXLSRowExtractor(POIFS,rowIdToRowDataFromSourceFile_Cache,sheetIndex,dataReadType,this.startRowNo);
				}else{
					extractRowDataFromXExcelFile = new FileReaderXLSRowExtractor(POIFS,rowIdToRowDataFromSourceFile_Cache,sheetName,dataReadType,this.startRowNo);	
				}
				extractRowDataFromXExcelFile.process();
			}finally{
				POIFS.close();
			}
			
		}else if(fileType == FileType.CSV){
			/*
			 * Previously we were using au parser, problem occurred in 8882 report as one of the column values contained '"'(double quote in between) and so it failing to reading correct cell values.
			 * So this new csv parser is introduced univocity, which overcomes the mentioned problem.
			 */
			CsvParserSettings csvSettings = new CsvParserSettings();
			csvSettings.setLineSeparatorDetectionEnabled(true);
			CsvParser csvParser = new CsvParser(csvSettings);
			if(sourceFile==null){
				csvParser.beginParsing(inputStream);								
			}else{
				csvParser.beginParsing(new java.io.FileReader(sourceFile));				
			}
			try{
				outputMap = getCSVDataBasedOnOrientation(csvParser,dataReadType,isCheckRowOrientation,rowMax,colMax,nextStartRow,nextStartCol);
			}finally{
				try{
					csvParser.stopParsing();					
				}catch(Exception e){
					e.printStackTrace();
					if(sourceFile==null){
						System.out.println("Exception occurred while stopping parsing for file "+urlPath);						
					}else{
						System.out.println("Exception occurred while stopping parsing for file "+sourceFile.getName());												
					}
				}finally{
					csvParser = null;					
				}
			}
		}else if(fileType == FileType.ZIP){
			ZipFile zipFile = new ZipFile(sourceFile);
			try{
				Enumeration zipEntries = zipFile.entries();
				if(zipEntries.hasMoreElements()){
					ZipEntry zipEntry = (ZipEntry)zipEntries.nextElement();
					if(!zipEntry.isDirectory()){
						InputStream inputStream = zipFile.getInputStream(zipEntry);
//						ZipInputStream zipInputStream = new ZipInputStream(inputStream);
						InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
						BufferedReader br = new BufferedReader(inputStreamReader);
						/*
						 * Previously we were using au parser, problem occurred in 8882 report as one of the column values contained '"'(double quote in between) and so it failing to reading correct cell values.
						 * So this new csv parser is introduced univocity, which overcomes the mentioned problem.
						 */
						CsvParserSettings csvSettings = new CsvParserSettings();
						CsvParser csvParser = new CsvParser(csvSettings);
						csvParser.beginParsing(br);
//						CSVReader csvReader = new CSVReader(br);
						try{
							getCSVRowIdToRowDataFromSourceFile(csvParser,dataReadType);
						}finally{
							try{
								csvParser.stopParsing();					
							}catch(Exception e){
								e.printStackTrace();
								System.out.println("Exception occurred while stopping parsing for file "+sourceFile.getName());
							}finally{
								csvParser = null;					
							}
							br.close();
							inputStreamReader.close();
//							zipInputStream.close();
							inputStream.close();
						}					
					}
				}
			}finally{
				zipFile.close();				
			}
		}else{
			throw new Exception(fileType.toString()+": This file type is not yet supported");
		}
		return outputMap;
	}

	public String getFileOrientationByValueRepetition(DataReadType dataReadType) throws Exception{
		String orientation = null;
		FileType fileType = getFileType();
//		Map<Integer,List<String>> rowIdToRowData = new HashMap<Integer,List<String>>();
		if(fileType == FileType.XLSX || fileType == FileType.XLSM){
			OPCPackage p;
			if(sourceFile==null){
				p = OPCPackage.open(inputStream);				
			}else{
				p = OPCPackage.open(sourceFile.getPath(), PackageAccess.READ);				
			}
			try{
				FileReaderXLSXMRowExtractor extractRowDataFromExcelFile = new FileReaderXLSXMRowExtractor(rowIdToRowDataFromSourceFile_Cache, this.includeColumnsIndexList,dataReadType,this.startRowNo, true, null);
				InputStream sheetInputStream = null;
				ReadOnlySharedStringsTable strings = new ReadOnlySharedStringsTable(p);
				XSSFReader xssfReader = new XSSFReader(p);
				StylesTable styles = xssfReader.getStylesTable();
				if(this.sheetName == null){
					sheetInputStream = extractRowDataFromExcelFile.getSheetInputStreamBySheetIndex(xssfReader,sheetIndex);
				}else{
					sheetInputStream = extractRowDataFromExcelFile.getSheetInputStreamBySheetName(xssfReader,sheetName);
				}
//				orientation = extractRowDataFromExcelFile.checkDataOrientation(styles, strings,extractRowDataFromExcelFile.new CustomSheetContentHandler(), sheetInputStream);
			}finally{
				p.close();
			}

		}else if(fileType == FileType.XLS){
			POIFSFileSystem POIFS = new POIFSFileSystem(sourceFile);
			try{
				FileReaderXLSRowExtractor extractRowDataFromXExcelFile = null;
				if(this.sheetName == null){
					extractRowDataFromXExcelFile = new FileReaderXLSRowExtractor(POIFS,rowIdToRowDataFromSourceFile_Cache,sheetIndex,dataReadType,this.startRowNo);
				}else{
					extractRowDataFromXExcelFile = new FileReaderXLSRowExtractor(POIFS,rowIdToRowDataFromSourceFile_Cache,sheetName,dataReadType,this.startRowNo);	
				}
				extractRowDataFromXExcelFile.process();
			}finally{
				POIFS.close();
			}
			
		}else if(fileType == FileType.CSV){
			/*
			 * Previously we were using au parser, problem occurred in 8882 report as one of the column values contained '"'(double quote in between) and so it failing to reading correct cell values.
			 * So this new csv parser is introduced univocity, which overcomes the mentioned problem.
			 */
			CsvParserSettings csvSettings = new CsvParserSettings();
			csvSettings.setLineSeparatorDetectionEnabled(true);
			CsvParser csvParser = new CsvParser(csvSettings);
			if(sourceFile==null){
				csvParser.beginParsing(inputStream);								
			}else{
				csvParser.beginParsing(new java.io.FileReader(sourceFile));				
			}
//			CSVReader csvReader = new CSVReader(new java.io.FileReader(sourceFile));
			try{
				getCSVRowIdToRowDataFromSourceFile(csvParser,dataReadType);
			}finally{
				try{
					csvParser.stopParsing();					
				}catch(Exception e){
					e.printStackTrace();
					if(sourceFile==null){
						System.out.println("Exception occurred while stopping parsing for file "+urlPath);						
					}else{
						System.out.println("Exception occurred while stopping parsing for file "+sourceFile.getName());												
					}
				}finally{
					csvParser = null;					
				}
			}
		}else if(fileType == FileType.ZIP){
			ZipFile zipFile = new ZipFile(sourceFile);
			try{
				Enumeration zipEntries = zipFile.entries();
				if(zipEntries.hasMoreElements()){
					ZipEntry zipEntry = (ZipEntry)zipEntries.nextElement();
					if(!zipEntry.isDirectory()){
						InputStream inputStream = zipFile.getInputStream(zipEntry);
//						ZipInputStream zipInputStream = new ZipInputStream(inputStream);
						InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
						BufferedReader br = new BufferedReader(inputStreamReader);
						/*
						 * Previously we were using au parser, problem occurred in 8882 report as one of the column values contained '"'(double quote in between) and so it failing to reading correct cell values.
						 * So this new csv parser is introduced univocity, which overcomes the mentioned problem.
						 */
						CsvParserSettings csvSettings = new CsvParserSettings();
						CsvParser csvParser = new CsvParser(csvSettings);
						csvParser.beginParsing(br);
//						CSVReader csvReader = new CSVReader(br);
						try{
							getCSVRowIdToRowDataFromSourceFile(csvParser,dataReadType);
						}finally{
							try{
								csvParser.stopParsing();					
							}catch(Exception e){
								e.printStackTrace();
								System.out.println("Exception occurred while stopping parsing for file "+sourceFile.getName());
							}finally{
								csvParser = null;					
							}
							br.close();
							inputStreamReader.close();
//							zipInputStream.close();
							inputStream.close();
						}					
					}
				}
			}finally{
				zipFile.close();				
			}
		}else{
			throw new Exception(fileType.toString()+": This file type is not yet supported");
		}
		return orientation;
	}

	public Map<Integer,List<String>> getRowIdToRowDataFromSourceFile(DataReadType dataReadType) throws Exception{
		if(rowIdToRowDataFromSourceFile_Cache != null){
			return rowIdToRowDataFromSourceFile_Cache;
		}else{
			rowIdToRowDataFromSourceFile_Cache = new HashMap<Integer,List<String>>();
		}
		
		FileType fileType = getFileType();
//		Map<Integer,List<String>> rowIdToRowData = new HashMap<Integer,List<String>>();
		if(fileType == FileType.XLSX || fileType == FileType.XLSM){
			OPCPackage p;
			if(sourceFile==null){
				p = OPCPackage.open(inputStream);				
			}else{
				p = OPCPackage.open(sourceFile.getPath(), PackageAccess.READ);				
			}
			try{
				FileReaderXLSXMRowExtractor extractRowDataFromExcelFile = new FileReaderXLSXMRowExtractor(rowIdToRowDataFromSourceFile_Cache, this.includeColumnsIndexList,dataReadType,this.startRowNo,false,null);
				InputStream sheetInputStream = null;
				ReadOnlySharedStringsTable strings = new ReadOnlySharedStringsTable(p);
				XSSFReader xssfReader = new XSSFReader(p);
				StylesTable styles = xssfReader.getStylesTable();
				if(this.sheetName == null){
					sheetInputStream = extractRowDataFromExcelFile.getSheetInputStreamBySheetIndex(xssfReader,sheetIndex);
				}else{
					sheetInputStream = extractRowDataFromExcelFile.getSheetInputStreamBySheetName(xssfReader,sheetName);
				}
				extractRowDataFromExcelFile.processSheet(styles, strings,extractRowDataFromExcelFile.new CustomSheetContentHandler(), sheetInputStream);
			}finally{
				p.close();
			}

		}else if(fileType == FileType.XLS){
			POIFSFileSystem POIFS = new POIFSFileSystem(sourceFile);
			try{
				FileReaderXLSRowExtractor extractRowDataFromXExcelFile = null;
				if(this.sheetName == null){
					extractRowDataFromXExcelFile = new FileReaderXLSRowExtractor(POIFS,rowIdToRowDataFromSourceFile_Cache,sheetIndex,dataReadType,this.startRowNo);
				}else{
					extractRowDataFromXExcelFile = new FileReaderXLSRowExtractor(POIFS,rowIdToRowDataFromSourceFile_Cache,sheetName,dataReadType,this.startRowNo);	
				}
				extractRowDataFromXExcelFile.process();
			}finally{
				POIFS.close();
			}
			
		}else if(fileType == FileType.CSV){
			/*
			 * Previously we were using au parser, problem occurred in 8882 report as one of the column values contained '"'(double quote in between) and so it failing to reading correct cell values.
			 * So this new csv parser is introduced univocity, which overcomes the mentioned problem.
			 */
			CsvParserSettings csvSettings = new CsvParserSettings();
			csvSettings.setLineSeparatorDetectionEnabled(true);
			CsvParser csvParser = new CsvParser(csvSettings);
			if(sourceFile==null){
				csvParser.beginParsing(inputStream);								
			}else{
				csvParser.beginParsing(new java.io.FileReader(sourceFile));				
			}
//			CSVReader csvReader = new CSVReader(new java.io.FileReader(sourceFile));
			try{
				getCSVRowIdToRowDataFromSourceFile(csvParser,dataReadType);
			}finally{
				try{
					csvParser.stopParsing();					
				}catch(Exception e){
					e.printStackTrace();
					if(sourceFile==null){
						System.out.println("Exception occurred while stopping parsing for file "+urlPath);						
					}else{
						System.out.println("Exception occurred while stopping parsing for file "+sourceFile.getName());												
					}
				}finally{
					csvParser = null;					
				}
			}
		}else if(fileType == FileType.ZIP){
			ZipFile zipFile = new ZipFile(sourceFile);
			try{
				Enumeration zipEntries = zipFile.entries();
				if(zipEntries.hasMoreElements()){
					ZipEntry zipEntry = (ZipEntry)zipEntries.nextElement();
					if(!zipEntry.isDirectory()){
						InputStream inputStream = zipFile.getInputStream(zipEntry);
//						ZipInputStream zipInputStream = new ZipInputStream(inputStream);
						InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
						BufferedReader br = new BufferedReader(inputStreamReader);
						/*
						 * Previously we were using au parser, problem occurred in 8882 report as one of the column values contained '"'(double quote in between) and so it failing to reading correct cell values.
						 * So this new csv parser is introduced univocity, which overcomes the mentioned problem.
						 */
						CsvParserSettings csvSettings = new CsvParserSettings();
						CsvParser csvParser = new CsvParser(csvSettings);
						csvParser.beginParsing(br);
//						CSVReader csvReader = new CSVReader(br);
						try{
							getCSVRowIdToRowDataFromSourceFile(csvParser,dataReadType);
						}finally{
							try{
								csvParser.stopParsing();					
							}catch(Exception e){
								e.printStackTrace();
								System.out.println("Exception occurred while stopping parsing for file "+sourceFile.getName());
							}finally{
								csvParser = null;					
							}
							br.close();
							inputStreamReader.close();
//							zipInputStream.close();
							inputStream.close();
						}					
					}
				}
			}finally{
				zipFile.close();				
			}
		}else{
			throw new Exception(fileType.toString()+": This file type is not yet supported");
		}
//		rowIdToRowDataFromSourceFile_Cache = rowIdToRowData;
//		rowIdToRowData = null;
		return rowIdToRowDataFromSourceFile_Cache;
	}
	
	public Map<Integer,List<String>> getRowIdToRowDataFromSourceFileWithOrientation(DataReadType dataReadType) throws Exception{
		Map<Integer, List<String>> outputMap = new HashMap<Integer, List<String>>();
		FileType fileType = getFileType();
//		Map<Integer,List<String>> rowIdToRowData = new HashMap<Integer,List<String>>();
		if(fileType == FileType.XLSX || fileType == FileType.XLSM){
			OPCPackage p;
			if(sourceFile==null){
				p = OPCPackage.open(inputStream);				
			}else{
				p = OPCPackage.open(sourceFile.getPath(), PackageAccess.READ);				
			}
			try{
				FileReaderXLSXMRowExtractor extractRowDataFromExcelFile = new FileReaderXLSXMRowExtractor(rowIdToRowDataFromSourceFile_Cache, this.includeColumnsIndexList,dataReadType,this.startRowNo,false,orientation);
				InputStream sheetInputStream = null;
				ReadOnlySharedStringsTable strings = new ReadOnlySharedStringsTable(p);
				XSSFReader xssfReader = new XSSFReader(p);
				StylesTable styles = xssfReader.getStylesTable();
				if(this.sheetName == null){
					sheetInputStream = extractRowDataFromExcelFile.getSheetInputStreamBySheetIndex(xssfReader,sheetIndex);
				}else{
					sheetInputStream = extractRowDataFromExcelFile.getSheetInputStreamBySheetName(xssfReader,sheetName);
				}
				extractRowDataFromExcelFile.processSheet(styles, strings,extractRowDataFromExcelFile.new CustomSheetContentHandler(), sheetInputStream);
			}finally{
				p.close();
			}

		}else if(fileType == FileType.XLS){
			POIFSFileSystem POIFS = new POIFSFileSystem(sourceFile);
			try{
				FileReaderXLSRowExtractor extractRowDataFromXExcelFile = null;
				if(this.sheetName == null){
					extractRowDataFromXExcelFile = new FileReaderXLSRowExtractor(POIFS,rowIdToRowDataFromSourceFile_Cache,sheetIndex,dataReadType,this.startRowNo);
				}else{
					extractRowDataFromXExcelFile = new FileReaderXLSRowExtractor(POIFS,rowIdToRowDataFromSourceFile_Cache,sheetName,dataReadType,this.startRowNo);	
				}
				extractRowDataFromXExcelFile.process();
			}finally{
				POIFS.close();
			}
			
		}else if(fileType == FileType.CSV){
			/*
			 * Previously we were using au parser, problem occurred in 8882 report as one of the column values contained '"'(double quote in between) and so it failing to reading correct cell values.
			 * So this new csv parser is introduced univocity, which overcomes the mentioned problem.
			 */
			CsvParserSettings csvSettings = new CsvParserSettings();
			csvSettings.setLineSeparatorDetectionEnabled(true);
			CsvParser csvParser = new CsvParser(csvSettings);
			if(sourceFile==null){
				csvParser.beginParsing(inputStream);								
			}else{
				csvParser.beginParsing(new java.io.FileReader(sourceFile));				
			}
//			CSVReader csvReader = new CSVReader(new java.io.FileReader(sourceFile));
			try{
				outputMap = getCSVFullDataBasedOnOrientation(csvParser);
			}finally{
				try{
					csvParser.stopParsing();					
				}catch(Exception e){
					e.printStackTrace();
					if(sourceFile==null){
						System.out.println("Exception occurred while stopping parsing for file "+urlPath);						
					}else{
						System.out.println("Exception occurred while stopping parsing for file "+sourceFile.getName());												
					}
				}finally{
					csvParser = null;					
				}
			}
		}else if(fileType == FileType.ZIP){
			ZipFile zipFile = new ZipFile(sourceFile);
			try{
				Enumeration zipEntries = zipFile.entries();
				if(zipEntries.hasMoreElements()){
					ZipEntry zipEntry = (ZipEntry)zipEntries.nextElement();
					if(!zipEntry.isDirectory()){
						InputStream inputStream = zipFile.getInputStream(zipEntry);
//						ZipInputStream zipInputStream = new ZipInputStream(inputStream);
						InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
						BufferedReader br = new BufferedReader(inputStreamReader);
						/*
						 * Previously we were using au parser, problem occurred in 8882 report as one of the column values contained '"'(double quote in between) and so it failing to reading correct cell values.
						 * So this new csv parser is introduced univocity, which overcomes the mentioned problem.
						 */
						CsvParserSettings csvSettings = new CsvParserSettings();
						CsvParser csvParser = new CsvParser(csvSettings);
						csvParser.beginParsing(br);
//						CSVReader csvReader = new CSVReader(br);
						try{
							getCSVRowIdToRowDataFromSourceFile(csvParser,dataReadType);
						}finally{
							try{
								csvParser.stopParsing();					
							}catch(Exception e){
								e.printStackTrace();
								System.out.println("Exception occurred while stopping parsing for file "+sourceFile.getName());
							}finally{
								csvParser = null;					
							}
							br.close();
							inputStreamReader.close();
//							zipInputStream.close();
							inputStream.close();
						}					
					}
				}
			}finally{
				zipFile.close();				
			}
		}else{
			throw new Exception(fileType.toString()+": This file type is not yet supported");
		}
//		rowIdToRowDataFromSourceFile_Cache = rowIdToRowData;
//		rowIdToRowData = null;
		return outputMap;
	}

	private Map<Integer,List<String>> getCSVRowIdToRowDataFromSourceFile(CsvParser csvParser,DataReadType dataReadType) throws IOException{
		int rowIndex = 0;
		String[] csvRowasArray = null;
		while ((csvRowasArray = csvParser.parseNext()) != null) {
			if(rowIndex >= this.startRowNo){
				if(includeColumnsIndexList != null){
					int colIndex = 0;
					rowIdToRowDataFromSourceFile_Cache.put(rowIndex, new ArrayList<String>());
					for(String cellValue : csvRowasArray){
						if(includeColumnsIndexList.contains(colIndex)){
							/*
							 * univocity csv parser puts '"' at the start and at end of the cell value if the value contains any '"', so it needs to be removed.
							 */
							if(cellValue!=null){
								if(cellValue.charAt(0) == '"'){
									cellValue = cellValue.substring(1, cellValue.length());
								}
								if(cellValue.charAt(cellValue.length()-1) == '"'){
									cellValue = cellValue.substring(0, cellValue.length()-1);
								}								
							}else{
								cellValue = "";
							}
							rowIdToRowDataFromSourceFile_Cache.get(rowIndex).add(cellValue);
						}
						colIndex++;
					}
				}else{
					/*
					 * univocity csv parser puts '"' at the start and at end of the cell value if the value contains any '"', so it needs to be removed.
					 */
					List<String> csvRowAsList = new ArrayList<String>();
					for(String cellValue : csvRowasArray){
						if(cellValue!=null){
							if(cellValue.charAt(0) == '"'){
								cellValue = cellValue.substring(1, cellValue.length());
							}
							if(cellValue.charAt(cellValue.length()-1) == '"'){
								cellValue = cellValue.substring(0, cellValue.length()-1);
							}							
						}else{
							cellValue = "";
						}
						csvRowAsList.add(cellValue);
					}
					rowIdToRowDataFromSourceFile_Cache.put(rowIndex, csvRowAsList);
				}
			}
			rowIndex ++;
			if(rowIdToRowDataFromSourceFile_Cache.size() == 1 && dataReadType == DataReadType.HEADER){
				break;
			}
			if(rowIdToRowDataFromSourceFile_Cache.size() == 2 && dataReadType == DataReadType.FIRSTROW){
				break;
			}
		}
		return rowIdToRowDataFromSourceFile_Cache;
	}
	
	private Map<Integer,List<String>> getCSVDataBasedOnOrientation(CsvParser csvParser,DataReadType dataReadType, boolean isCheckRowOrientation, int rowMax, int colMax, int nextStartRow, int nextStartCol) throws IOException{
		Map<Integer,List<String>> outputMap = new HashMap<Integer,List<String>>();
		int rowIndex = 0;
		String[] csvRowasArray = null;
		while ((csvRowasArray = csvParser.parseNext()) != null) {
			if((rowIndex>=nextStartRow) && (rowIndex<rowMax)){
				int colIndex = 0;				
				if(isCheckRowOrientation){
					outputMap.put(rowIndex, new ArrayList<String>());
					for(String cellValue : csvRowasArray){
						if((colIndex>=nextStartCol) && (colIndex<colMax)){
							/*
							 * univocity csv parser puts '"' at the start and at end of the cell value if the value contains any '"', so it needs to be removed.
							 */
							if(cellValue!=null){
								if(cellValue.charAt(0) == '"'){
									cellValue = cellValue.substring(1, cellValue.length());
								}
								if(cellValue.charAt(cellValue.length()-1) == '"'){
									cellValue = cellValue.substring(0, cellValue.length()-1);
								}								
							}else{
								cellValue = "";
							}
							outputMap.get(rowIndex).add(cellValue);
						}
						colIndex++;

						if((colIndex>=colMax)){
							break;
						}
					}					
				}else{
					for(String cellValue : csvRowasArray){
						if((colIndex>=nextStartCol) && (colIndex<colMax)){
							/*
							 * univocity csv parser puts '"' at the start and at end of the cell value if the value contains any '"', so it needs to be removed.
							 */
							if(cellValue!=null){
								if(cellValue.charAt(0) == '"'){
									cellValue = cellValue.substring(1, cellValue.length());
								}
								if(cellValue.charAt(cellValue.length()-1) == '"'){
									cellValue = cellValue.substring(0, cellValue.length()-1);
								}								
							}else{
								cellValue = "";
							}
							
							if(!outputMap.containsKey(colIndex)){
								outputMap.put(colIndex, new ArrayList<String>());								
							}
							outputMap.get(colIndex).add(cellValue);
						}
						colIndex++;

						if((colIndex>=colMax)){
							break;
						}
					}					
				}
			}
			rowIndex ++;
			
			if((rowIndex>=rowMax)){
				break;
			}
		}
		return outputMap;
	}

	private Map<Integer,List<String>> getCSVFullDataBasedOnOrientation(CsvParser csvParser) throws IOException{
		Map<Integer,List<String>> outputMap = new HashMap<Integer,List<String>>();
		int rowIndex = 0;
		String[] csvRowasArray = null;
		while ((csvRowasArray = csvParser.parseNext()) != null) {
			outputMap.put(rowIndex, new ArrayList<String>());
			for(String cellValue : csvRowasArray){
				/*
				 * univocity csv parser puts '"' at the start and at end of the cell value if the value contains any '"', so it needs to be removed.
				 */
				if(cellValue!=null){
					if(cellValue.charAt(0) == '"'){
						cellValue = cellValue.substring(1, cellValue.length());
					}
					if(cellValue.charAt(cellValue.length()-1) == '"'){
						cellValue = cellValue.substring(0, cellValue.length()-1);
					}								
				}else{
					cellValue = "";
				}
				outputMap.get(rowIndex).add(cellValue);
			}					
			rowIndex ++;
		}
		return outputMap;
	}

	@Override
	public List<String> readHeaderRowFromDocFile() throws Exception {
		Map<String, List<Map<String, String>>> returnSheetWiseRowDataList = getRowDataMapFromDocFile(DataReadType.FIRSTROW);
		List<String> headerList = new ArrayList<String>();

		try {
			for(Map.Entry<String, List<Map<String,String>>> e : returnSheetWiseRowDataList.entrySet()){
				List<Map<String,String>> value = e.getValue();
				if(value!=null){
					for(Map.Entry<String,String> rowMap : value.get(0).entrySet()){
						headerList.add(rowMap.getKey());
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error occured while reading first line of file : "+sourceFile!=null?sourceFile.getPath():urlPath);
			throw new Exception(e);
		}
		return headerList;	
	}

	@Override
	public Map<String, List<Map<String, String>>> readDataFromDocFile() throws Exception {
		Map<String, List<Map<String, String>>> returnSheetWiseRowDataList = getRowDataMapFromDocFile(DataReadType.ALL);

		return returnSheetWiseRowDataList;	
	}

	@Override
	public Map<String, String> readFirstRowFromDocFile() throws Exception {
		Map<String, List<Map<String, String>>> returnSheetWiseRowDataList = getRowDataMapFromDocFile(DataReadType.FIRSTROW);
		Map<String,String> firstRowMap = new HashMap<String,String>();

		try {
			for(Map.Entry<String, List<Map<String,String>>> e : returnSheetWiseRowDataList.entrySet()){
				List<Map<String,String>> value = e.getValue();
				if(value!=null){
					firstRowMap = value.get(0);
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error occured while reading first line of file : "+sourceFile!=null?sourceFile.getPath():urlPath);
			throw new Exception(e);
		}
		return firstRowMap;	
	}
	
	public Map<String, List<Map<String, String>>> getRowDataMapFromDocFile(DataReadType dataReadType) throws  Exception{
		if(sourceFile!=null){
			if (!this.sourceFile.exists()) {
				throw new Exception(sourceFile.getPath()+": File not found..");
			}			
		}

		if(sourceFile!=null){
			System.out.println(sourceFile.getPath()+": Reading started");			
		}else{
			System.out.println(urlPath+": Reading started");			
		}
		Map<String, List<Map<String, String>>> returnSheetWiseRowDataList = new HashMap<String, List<Map<String, String>>>();
		List<Map<String, String>> rowDataList = new ArrayList<Map<String, String>>();
		List<String> headerList = new ArrayList<String>();

		try {
			FileInputStream fis = new FileInputStream(sourceFile);
			XWPFDocument doc = new XWPFDocument(fis);
			List<XWPFTable> tableList = doc.getTables();
			for(XWPFTable table : tableList){
				int rowIndex = 0;
				for(XWPFTableRow row : table.getRows()){
					int colIndex = 0;
					Map<String,String> rowData = new HashMap<String,String>();
					for(XWPFTableCell cell : row.getTableCells()){
						if(rowIndex==0){
							if(concatenateIndex){
								headerList.add(cell.getText()+"_"+colIndex);
							}else{
								headerList.add(cell.getText());							
							}							
						}else{
							rowData.put(headerList.get(colIndex).trim(), cell.getText().trim());															
						}
						colIndex++;
					}
					if(rowIndex>0){	
						rowDataList.add(rowData);
						if(dataReadType.equals(DataReadType.FIRSTROW)){
							returnSheetWiseRowDataList.put(this.sheetName,rowDataList);	
							return returnSheetWiseRowDataList;
						}
					}
					rowIndex++;
				}
			}
			returnSheetWiseRowDataList.put(this.sheetName,rowDataList);	
		} catch (InvalidOperationException e) {
			e.printStackTrace();
			System.out.println("Error occured while reading file : "+sourceFile.getPath());
			throw new IOException(e);
		} finally{
			rowDataList = null;
		}

		if(sourceFile!=null){
			System.out.println(sourceFile.getPath()+": Reading ended");			
		}else{
			System.out.println(urlPath+": Reading ended");			
		}
		
		return returnSheetWiseRowDataList;			
	}

}

class FileReaderXLSRowExtractor implements HSSFListener {
	private int minColumns;
	private POIFSFileSystem fs;
//	private PrintStream output;

	private int lastRowNumber;
	private int lastColumnNumber;

	/** Should we output the formula, or the value it has? */
	private boolean outputFormulaValues = true;

	/** For parsing Formulas */
	private SheetRecordCollectingListener workbookBuildingListener;
	private HSSFWorkbook stubWorkbook;

	// Records we pick up as we process
	private SSTRecord sstRecord;
	private FormatTrackingHSSFListener formatListener;
	
	/** So we known which sheet we're on */
	private int currentSheetIndex = -1;
	private String currentSheetName = null;
	
	private BoundSheetRecord[] orderedBSRs;
	private List<BoundSheetRecord> boundSheetRecords = new ArrayList<BoundSheetRecord>();

	// For handling formulas with string results
	private int nextRow;
	private int nextColumn;
	private boolean outputNextStringRecord;
	private Map<Integer,List<String>> outputMap;
	private String requiredSheetName;
	private int requiredSheetIndex;
	private FileReaderMemoryOptimized.DataReadType dataReadType;
	private boolean exitRead;
	private int startRowNo;
	/**
	 * Creates a new XLS -> CSV converter
	 * @param fs The POIFSFileSystem to process
	 * @param output The PrintStream to output the CSV to
	 * @param minColumns The minimum number of columns to output, or -1 for no minimum
	 */
	public FileReaderXLSRowExtractor(POIFSFileSystem fs) {
		this.fs = fs;
	}
	public FileReaderXLSRowExtractor(POIFSFileSystem fs,Map<Integer,List<String>> outputMap,String sheetName,FileReaderMemoryOptimized.DataReadType dataReadType,int startRowNo) {
		this.fs = fs;
		this.outputMap = outputMap;
		this.requiredSheetName = sheetName;
		this.dataReadType = dataReadType;
		this.startRowNo = startRowNo;
	}
	public FileReaderXLSRowExtractor(POIFSFileSystem fs,Map<Integer,List<String>> outputMap,int sheetIndex,FileReaderMemoryOptimized.DataReadType dataReadType,int startRowNo) {
		this.fs = fs;
		this.outputMap = outputMap;
		this.requiredSheetIndex = sheetIndex;
		this.dataReadType = dataReadType;
		this.startRowNo = startRowNo;
	}
	
	public List<String> getTotalSheetList() throws IOException{
		MissingRecordAwareHSSFListener listener = new MissingRecordAwareHSSFListener(this);
		formatListener = new FormatTrackingHSSFListener(listener);
		List<String> sheetList = new ArrayList<String>();
		HSSFEventFactory factory = new HSSFEventFactory();
		HSSFRequest request = new HSSFRequest();
		request.addListener(formatListener, BoundSheetRecord.sid);
		factory.processWorkbookEvents(request, fs);
		for(BoundSheetRecord bsr : BoundSheetRecord.orderByBofPosition(boundSheetRecords)){
			sheetList.add(bsr.getSheetname());
		}
		return sheetList;
	}

	/**
	 * Initiates the processing of the XLS file to CSV
	 */
	public void process() throws IOException {
		MissingRecordAwareHSSFListener listener = new MissingRecordAwareHSSFListener(this);
		formatListener = new FormatTrackingHSSFListener(listener);

		HSSFEventFactory factory = new HSSFEventFactory();
		HSSFRequest request = new HSSFRequest();

		if(outputFormulaValues) {
			request.addListenerForAllRecords(formatListener);
		} else {
			workbookBuildingListener = new SheetRecordCollectingListener(formatListener);
			request.addListenerForAllRecords(workbookBuildingListener);
		}

		factory.processWorkbookEvents(request, fs);
	}

	/**
	 * Main HSSFListener method, processes events, and outputs the
	 *  CSV as the file is processed.
	 */
	@Override
	public void processRecord(Record record) {
		if(!exitRead){
			int thisRow = -1;
			int thisColumn = -1;
			String thisStr = null;
			if(requiredSheetName != null){
				if(currentSheetName != null && !requiredSheetName.equals(currentSheetName)){
					currentSheetName = null;
					return;
				}
			}else{
				if(currentSheetIndex >= 0 && currentSheetIndex != requiredSheetIndex){
					currentSheetIndex = -1;
					return;
				}
			}

			switch (record.getSid())
			{
			case BoundSheetRecord.sid:
				boundSheetRecords.add((BoundSheetRecord)record);
				break;
			case BOFRecord.sid:
				BOFRecord br = (BOFRecord)record;
				if(br.getType() == BOFRecord.TYPE_WORKSHEET) {
					// Create sub workbook if required
					if(workbookBuildingListener != null && stubWorkbook == null) {
						stubWorkbook = workbookBuildingListener.getStubHSSFWorkbook();
					}

					// Output the worksheet name
					// Works by ordering the BSRs by the location of
					//  their BOFRecords, and then knowing that we
					//  process BOFRecords in byte offset order
					currentSheetIndex++;
					if(orderedBSRs == null) {
						orderedBSRs = BoundSheetRecord.orderByBofPosition(boundSheetRecords);
					}
					currentSheetName = orderedBSRs[currentSheetIndex].getSheetname();
				}
				break;

			case SSTRecord.sid:
				sstRecord = (SSTRecord) record;
				break;

			case BlankRecord.sid:
				BlankRecord brec = (BlankRecord) record;

				thisRow = brec.getRow();
				thisColumn = brec.getColumn();
				thisStr = "";
				break;
			case BoolErrRecord.sid:
				BoolErrRecord berec = (BoolErrRecord) record;

				thisRow = berec.getRow();
				thisColumn = berec.getColumn();
				thisStr = "";
				break;

			case FormulaRecord.sid:
				FormulaRecord frec = (FormulaRecord) record;

				thisRow = frec.getRow();
				thisColumn = frec.getColumn();

				if(outputFormulaValues) {
					if(Double.isNaN( frec.getValue() )) {
						// Formula result is a string
						// This is stored in the next record
						outputNextStringRecord = true;
						nextRow = frec.getRow();
						nextColumn = frec.getColumn();
					} else {
						thisStr = formatListener.formatNumberDateCell(frec);
					}
				} else {
					//				thisStr = '"' +HSSFFormulaParser.toFormulaString(stubWorkbook, frec.getParsedExpression()) + '"';
					thisStr = HSSFFormulaParser.toFormulaString(stubWorkbook, frec.getParsedExpression());
				}
				break;
			case StringRecord.sid:
				if(outputNextStringRecord) {
					// String for formula
					StringRecord srec = (StringRecord)record;
					thisStr = srec.getString();
					thisRow = nextRow;
					thisColumn = nextColumn;
					outputNextStringRecord = false;
				}
				break;

			case LabelRecord.sid:
				LabelRecord lrec = (LabelRecord) record;

				thisRow = lrec.getRow();
				thisColumn = lrec.getColumn();
				//			thisStr = '"' + lrec.getValue() + '"';
				thisStr = lrec.getValue();
				break;
			case LabelSSTRecord.sid:
				LabelSSTRecord lsrec = (LabelSSTRecord) record;

				thisRow = lsrec.getRow();
				thisColumn = lsrec.getColumn();
				if(sstRecord == null) {
					//				thisStr = '"' + "(No SST Record, can't identify string)" + '"';
				} else {
					//				thisStr = '"' + sstRecord.getString(lsrec.getSSTIndex()).toString() + '"';
					thisStr = sstRecord.getString(lsrec.getSSTIndex()).toString();
				}
				break;
			case NoteRecord.sid:
				NoteRecord nrec = (NoteRecord) record;

				thisRow = nrec.getRow();
				thisColumn = nrec.getColumn();
				// TODO: Find object to match nrec.getShapeId()
				//			thisStr = '"' + "(TODO)" + '"';
				break;
			case NumberRecord.sid:
				NumberRecord numrec = (NumberRecord) record;
				thisRow = numrec.getRow();
				thisColumn = numrec.getColumn();
				double d = numrec.getValue();
				// Format
				String formattedValue = formatListener.formatNumberDateCell(numrec);
				thisStr = formattedValue;
//				try {
//					thisStr = String.valueOf(StringUtil.parseString(formattedValue));
//				} catch (ParseException e) {
//					thisStr = formattedValue;
//				}
				break;
			case RKRecord.sid:
				RKRecord rkrec = (RKRecord) record;

				thisRow = rkrec.getRow();
				thisColumn = rkrec.getColumn();
				//			thisStr = '"' + "(TODO)" + '"';
				break;
			default:
				break;
			}

			// Handle new row
			if(thisRow != -1 && thisRow != lastRowNumber) {
				lastColumnNumber = -1;
			}

			// Handle missing column
			if(record instanceof MissingCellDummyRecord) {
				MissingCellDummyRecord mc = (MissingCellDummyRecord)record;
				thisRow = mc.getRow();
				thisColumn = mc.getColumn();
				thisStr = "";
			}

			// If we got something to print out, do so
			if(thisStr != null && thisRow >= startRowNo) {
				if(!outputMap.containsKey(thisRow)){
					outputMap.put(thisRow,new ArrayList<String>());
				}
				outputMap.get(thisRow).add(thisStr);
				//			output.print(thisStr);
			}

			// Update column and row count
			if(thisRow > -1)
				lastRowNumber = thisRow;
			if(thisColumn > -1)
				lastColumnNumber = thisColumn;

			// Handle end of row
			if(record instanceof LastCellOfRowDummyRecord) {
				// Print out any missing commas if needed
				if(minColumns > 0 && lastRowNumber >= startRowNo) {
					// Columns are 0 based
					if(lastColumnNumber == -1) { lastColumnNumber = 0; }
					for(int i=lastColumnNumber; i<(minColumns); i++) {
						//					output.print(',');
						if(!outputMap.containsKey(lastRowNumber)){
							outputMap.put(lastRowNumber,new ArrayList<String>());
						}
						outputMap.get(lastRowNumber).add("");
					}
				}

				// We're onto a new row
				lastColumnNumber = -1;
				if(outputMap.size() == 1){
					minColumns = outputMap.get(lastRowNumber).size();
				}
				if(dataReadType == FileReaderMemoryOptimized.DataReadType.FIRSTROW && outputMap.size() == 2){
					exitRead = true;
				}
				if(dataReadType == FileReaderMemoryOptimized.DataReadType.HEADER && outputMap.size() == 1){
					exitRead = true;
				}
				// End the row
				// output.println();
			}
		}
	}

}

class FileReaderXLSXMRowExtractor {
	/**
	 * Uses the XSSF Event SAX helpers to do most of the work
	 *  of parsing the Sheet XML, and outputs the contents
	 *  as a (basic) CSV.
	 */
	public class CustomSheetContentHandler implements SheetContentsHandler {
		private int currentRow = -1;
		private int currentCol = -1;

		@Override
		public void startRow(int rowNum) {
			if(isCheckDataOrientation){
				if(!exitRead && rowNum >= startRowNo && rowNum < rowMax){
					
					// Prepare for this row
					currentRow = rowNum;
					currentCol = -1;		
				}else if(rowNum >= rowMax){
					exitRead = true;
				}				
			}else{
				if(!exitRead && rowNum >= startRowNo){
					
					// Prepare for this row
					if(!outputMap.containsKey(rowNum)){
						outputMap.put(rowNum, new ArrayList<String>());						
					}
					currentRow = rowNum;
					currentCol = -1;		
				}				
			}
		}

		@Override
		public void endRow(int rowNum) {
			if(outputMap.size() == 1){
				if(outputMap.get(currentRow)!=null){
					minHeaderColumns = outputMap.get(currentRow).size();					
				}
			}
			if(dataReadType == FileReaderMemoryOptimized.DataReadType.FIRSTROW && outputMap.size() == 2){
				exitRead = true;
			}
			if(dataReadType == FileReaderMemoryOptimized.DataReadType.HEADER && outputMap.size() == 1){
				exitRead = true;
			}
			
			if(isCheckDataOrientation){
				if(rowNum >= startRowNo && rowNum < rowMax){
					int missingCols = minHeaderColumns - currentCol;
					if(missingCols > 0){
						for(int i=0;i<missingCols;i++){
							if((currentCol+i) >= startColNo && (currentCol+i) < colMax){
								if(isCheckRowOrientation){
									if(!outputMap.containsKey(currentRow)){
										outputMap.put(currentRow, new ArrayList<String>());								
									}
									outputMap.get(currentRow).add("");									
								}else{
									if(!outputMap.containsKey(currentCol+i)){
										outputMap.put(currentCol+i, new ArrayList<String>());								
									}
									outputMap.get(currentCol+i).add("");									
								}
							}
						}							
					}															
				}
			}else{
				int missingCols = minHeaderColumns - (currentCol + 1);
				if(missingCols > 0){
					for(int addC =0;addC<missingCols;addC++){
						outputMap.get(currentRow).add("");
					}
				}				
			}

		}

		@Override
		public void cell(String cellReference, String formattedValue,XSSFComment comment) {
			if(isCheckDataOrientation){
				if(cellReference != null) {
					int col = (new CellReference(cellReference)).getCol();
					if(col>minHeaderColumns){
						minHeaderColumns = col;
					}
				}
				if(!exitRead && currentRow >= startRowNo){
					if(cellReference == null) {
						cellReference = new CellAddress(currentRow, currentCol).formatAsString();
					}
					int thisCol = (new CellReference(cellReference)).getCol();
					int missedCols = thisCol - currentCol - 1;
					
					if(isCheckRowOrientation){
						for (int i=1; i<=missedCols; i++) {
							if((currentCol+i) >= startColNo && (currentCol+i) < colMax){
								if(!outputMap.containsKey(currentRow)){
									outputMap.put(currentRow, new ArrayList<String>());								
								}
								outputMap.get(currentRow).add("");								
							}
						}
						currentCol = thisCol;
						if(includeColumnList == null || includeColumnList.contains(currentCol)){
							// Number or string?
							if(currentCol >= startColNo && currentCol < colMax){
								if(!outputMap.containsKey(currentRow)){
									outputMap.put(currentRow, new ArrayList<String>());								
								}
								outputMap.get(currentRow).add(formattedValue);								
							}
						}						
					}else{
						if(currentCol>=0){
							for (int i=1; i<=missedCols; i++) {
								if((currentCol+i) >= startColNo && (currentCol+i) < colMax){
									if(!outputMap.containsKey(currentCol+i)){
										outputMap.put(currentCol+i, new ArrayList<String>());								
									}
									outputMap.get(currentCol+i).add("");									
								}
							}							
						}
						currentCol = thisCol;
						if(includeColumnList == null || includeColumnList.contains(currentCol)){
							// Number or string?
							if(currentCol >= startColNo && currentCol < colMax){
								if(!outputMap.containsKey(currentCol)){
									outputMap.put(currentCol, new ArrayList<String>());								
								}
								outputMap.get(currentCol).add(formattedValue);								
							}
						}
					}
				}				
			}else{
				if(orientation==null || !orientation.equalsIgnoreCase("Row Orientation")){
					if(!exitRead && currentRow >= startRowNo){
						if(cellReference == null) {
							cellReference = new CellAddress(currentRow, currentCol).formatAsString();
						}
						int thisCol = (new CellReference(cellReference)).getCol();
						int missedCols = thisCol - currentCol - 1;
						for (int i=0; i<missedCols; i++) {
							outputMap.get(currentRow).add("");
						}
						currentCol = thisCol;
						if(includeColumnList == null || includeColumnList.contains(currentCol)){
							// Number or string?
							outputMap.get(currentRow).add(formattedValue);							
						}
					}									
				}else{
					if(!exitRead && currentRow >= startRowNo){
						if(cellReference == null) {
							cellReference = new CellAddress(currentRow, currentCol).formatAsString();
						}
						int thisCol = (new CellReference(cellReference)).getCol();
						int missedCols = thisCol - currentCol - 1;
						if(currentCol>=0){
							for (int i=currentCol; i<missedCols; i++) {
								if(!outputMap.containsKey(currentCol)){
									outputMap.put(currentCol, new ArrayList<String>());								
								}
								outputMap.get(currentCol).add("");
							}							
						}
						currentCol = thisCol;
						if(includeColumnList == null || includeColumnList.contains(currentCol)){
							// Number or string?
							if(!outputMap.containsKey(currentCol)){
								outputMap.put(currentCol, new ArrayList<String>());								
							}
							outputMap.get(currentCol).add(formattedValue);								
						}
					}				
				}
			}
		}

		@Override
		public void headerFooter(String text, boolean isHeader, String tagName) {
		}
	}


	/**
	 * Number of columns to read starting with leftmost
	 */
	private int minHeaderColumns;

	/**
	 * Destination for data
	 */
//	private final PrintStream output;
	private Map<Integer,List<String>> outputMap ;
	private Set<Integer> includeColumnList;
	private FileReaderMemoryOptimized.DataReadType dataReadType;
	private boolean exitRead;
	private int startRowNo;
	private boolean isCheckDataOrientation;
	private boolean isCheckRowOrientation;
	private String orientation;
	private int rowMax = 2;
	private int colMax = 4;
	private int startColNo;
	/**
	 * Creates a new XLSX -> CSV converter
	 *
	 * @param pkg        The XLSX package to process
	 * @param output     The PrintStream to output the CSV to
	 * @param minColumns The minimum number of columns to output, or -1 for no minimum
	 */
	public FileReaderXLSXMRowExtractor(Map<Integer,List<String>> outputMap,Set<Integer> includeColumnList,FileReaderMemoryOptimized.DataReadType dataReadType,int startRowNo, boolean isCheckDataOrientation, String orientation) {
		this.outputMap = outputMap;  
		this.includeColumnList = includeColumnList;
		this.dataReadType = dataReadType;
		this.startRowNo = startRowNo;
		this.isCheckDataOrientation = isCheckDataOrientation;
		this.orientation = orientation;
	}

	public FileReaderXLSXMRowExtractor(Map<Integer,List<String>> outputMap,Set<Integer> includeColumnList,FileReaderMemoryOptimized.DataReadType dataReadType,int startRowNo, boolean isCheckDataOrientation, boolean isCheckRowOrientation, String orientation, int rowMax, int colMax, int startColNo) {
		this.outputMap = outputMap;  
		this.includeColumnList = includeColumnList;
		this.dataReadType = dataReadType;
		this.startRowNo = startRowNo;
		this.isCheckDataOrientation = isCheckDataOrientation;
		this.isCheckRowOrientation = isCheckRowOrientation;
		this.orientation = orientation;
		this.rowMax = rowMax;
		this.colMax = colMax;
		this.startColNo = startColNo;
	}

	/**
	 * Parses and shows the content of one sheet
	 * using the specified styles and shared-strings tables.
	 *
	 * @param styles The table of styles that may be referenced by cells in the sheet
	 * @param strings The table of strings that may be referenced by cells in the sheet
	 * @param sheetInputStream The stream to read the sheet-data from.

	 * @exception java.io.IOException An IO exception from the parser,
	 *            possibly from a byte stream or character stream
	 *            supplied by the application.
	 * @throws SAXException if parsing the XML data fails.
	 */
	public void processSheet(
			StylesTable styles,
			ReadOnlySharedStringsTable strings,
			SheetContentsHandler sheetHandler, 
			InputStream sheetInputStream) throws IOException, SAXException {
		DataFormatter formatter = new DataFormatter();
		InputSource sheetSource = new InputSource(sheetInputStream);
		try {
			XMLReader sheetParser = SAXHelper.newXMLReader();
			ContentHandler handler = new XSSFSheetXMLHandler(
					styles, null, strings, sheetHandler, formatter, false);
			sheetParser.setContentHandler(handler);
			sheetParser.parse(sheetSource);
		} catch(ParserConfigurationException e) {
			throw new RuntimeException("SAX parser appears to be broken - " + e.getMessage());
		}
	}

	public Map<Integer,List<String>> checkDataOrientation(
			StylesTable styles,
			ReadOnlySharedStringsTable strings,
			SheetContentsHandler sheetHandler, 
			InputStream sheetInputStream) throws IOException, SAXException {
		DataFormatter formatter = new DataFormatter();
		InputSource sheetSource = new InputSource(sheetInputStream);
		try {
			XMLReader sheetParser = SAXHelper.newXMLReader();
			ContentHandler handler = new XSSFSheetXMLHandler(
					styles, null, strings, sheetHandler, formatter, false);
			sheetParser.setContentHandler(handler);
			sheetParser.parse(sheetSource);
			return outputMap;
		} catch(ParserConfigurationException e) {
			throw new RuntimeException("SAX parser appears to be broken - " + e.getMessage());
		}
	}

	public static List<String> getTotalSheetList(XSSFReader xssfReader) throws InvalidFormatException, IOException{
		XSSFReader.SheetIterator iter = (XSSFReader.SheetIterator) xssfReader.getSheetsData();
		List<String> sheetNameList = new ArrayList<String>();
		while (iter.hasNext()) {
			iter.next();
			sheetNameList.add(iter.getSheetName());
		}
		return sheetNameList;
	}
	
	public InputStream getSheetInputStreamBySheetIndex(XSSFReader xssfReader,int sheetIndex) throws InvalidFormatException, IOException{
		XSSFReader.SheetIterator iter = (XSSFReader.SheetIterator) xssfReader.getSheetsData();
		int index = 0;
		while (iter.hasNext()) {
			InputStream is = iter.next();
			if(index == sheetIndex){
				return is;
			}
			index ++;
		}
		return null;
	}
	
	public InputStream getSheetInputStreamBySheetName(XSSFReader xssfReader,String sheetName) throws InvalidFormatException, IOException{
		XSSFReader.SheetIterator iter = (XSSFReader.SheetIterator) xssfReader.getSheetsData();
		while (iter.hasNext()) {
			InputStream is = iter.next();
			if(iter.getSheetName().trim().equalsIgnoreCase(sheetName.trim())){
				return is;
			}
		}
		return null;
	}
}

interface NodeTree {
	
	public boolean isIncludeNode();

	public void setIncludeNode(boolean includeNode);
	
	public Node getCurrentNode();

	public void setCurrentNode(Node currentNode);
	
	public Node getParentNode();

	public void setParentNode(Node parentNode);

	public List<NodeTree> getDependTreeList();

	public void setDependTreeList(List<NodeTree> dependTreeList);
	
	public void addDependTree(NodeTree nodeTree);
	
	public boolean isRootNode();

	public void setRootNode(boolean isRootNode);
	
	public boolean isShowCheckedMark();

	public void setShowCheckedMark(boolean showCheckedMark);
	
	public NodeTree getSubTree(String nodeId);
	
	public List<Node> getAllTreeNodes();
	
	public List<Node> getTerminalNodes();
	
	public Set<String> getRelationTypeUp() ;

	public void setRelationTypeUp(Set<String> relationTypeUp);

	public Set<String> getRelationTypeDown() ;
	
	public void setRelationTypeDown(Set<String> relationTypeDown) ;
	public void addRelationTypeDown(String relationTypeDown) ;
	public void addRelationTypeUp(String relationTypeUp);
	
	public JSONObject toJSONObject() throws JSONException;
	
	public String toString();
}

class NodeTreeImpl implements NodeTree, Serializable {
	private static final long serialVersionUID = 1L;
	
	private Node currentNode;
	private List<NodeTree> dependTreeList;
	private Node parentNode;
	private boolean isRootNode;
	private boolean showCheckedMark;
	private boolean includeNode;
	private Set<String> relationTypeUp;
	private Set<String> relationTypeDown;
	
	public NodeTreeImpl() {
	}

	public NodeTreeImpl(Node currentNode, List<NodeTree> dependTreeList) {
		this.currentNode = currentNode;
		this.dependTreeList = dependTreeList;
	}

	public NodeTreeImpl(String jsonString) throws JSONException {
		JSONObject jsonObject = new JSONObject(jsonString);
		
		if(jsonObject.optJSONArray("data") != null) {
			JSONArray jsonArray = (JSONArray) jsonObject.optJSONArray("data");
			List<NodeTree> nodeTreeList = new ArrayList<NodeTree>();
			
			for(int i=0; i < jsonArray.length(); i++) {
				JSONObject jsonObject2 = (JSONObject) jsonArray.get(i);
				NodeTree nodeTree = new NodeTreeImpl();
				
				if(jsonObject2.optJSONArray("children") != null && jsonObject2.optJSONArray("children").length() > 0) {
					JSONArray childArray = jsonObject2.optJSONArray("children");
					List<NodeTree> nodeTreeList2 = new ArrayList<NodeTree>();
					
					for(int j=0; j < childArray.length(); j++) {
						JSONObject childJSONObject = (JSONObject) childArray.get(j);
						NodeTree nodeTree1 = new NodeTreeImpl(childJSONObject.toString());
						nodeTreeList2.add(nodeTree1);
					}
					
					nodeTree.setDependTreeList(nodeTreeList2);
				}
				
				Node node = new NodeImpl();
				JSONObject attributeJsonObject = jsonObject2.optJSONObject("attr");
				node.setId(attributeJsonObject.optString("id"));
				node.setName(attributeJsonObject.optString("name"));
				nodeTree.setCurrentNode(node);
				nodeTreeList.add(nodeTree);
			}
			
			if(this.dependTreeList == null) {
				this.dependTreeList = new ArrayList<NodeTree>();
			}
			
			this.dependTreeList = nodeTreeList;
			this.isRootNode = true;
			
		} else {
			JSONObject jsonAttribute = jsonObject.optJSONObject("attr");
			Node node = new NodeImpl();
			node.setId(jsonAttribute.optString("id"));
			node.setName(jsonAttribute.optString("name"));
			
			this.currentNode = node;
			
			if(jsonObject.optJSONArray("children") != null && jsonObject.optJSONArray("children").length() > 0) {
				JSONArray childArray = jsonObject.optJSONArray("children");
				List<NodeTree> nodeTreeList2 = new ArrayList<NodeTree>();
				
				for(int j=0; j < childArray.length(); j++) {
					JSONObject childJSONObject = (JSONObject) childArray.get(j);
					NodeTree nodeTree1 = new NodeTreeImpl(childJSONObject.toString());
					nodeTreeList2.add(nodeTree1);
				}
				
				this.setDependTreeList(nodeTreeList2);
			}
		}
	}
	
	
	public boolean isIncludeNode() {
		return includeNode;
	}

	public void setIncludeNode(boolean includeNode) {
		this.includeNode = includeNode;
	}
	
	public Node getParentNode() {
		return parentNode;
	}

	public void setParentNode(Node parentNode) {
		this.parentNode = parentNode;
	}
	
	public Node getCurrentNode() {
		return currentNode;
	}

	public void setCurrentNode(Node currentNode) {
		this.currentNode = currentNode;
	}

	public List<NodeTree> getDependTreeList() {
		return dependTreeList;
	}

	public void setDependTreeList(List<NodeTree> dependTreeList) {
		this.dependTreeList = dependTreeList;
	}
	
	public void addDependTree(NodeTree nodeTree) {
		if(this.dependTreeList == null) {
			this.dependTreeList = new ArrayList<NodeTree>();
		}
		this.dependTreeList.add(nodeTree);
	}

	public boolean isRootNode() {
		return isRootNode;
	}

	public void setRootNode(boolean isRootNode) {
		this.isRootNode = isRootNode;
	}
	
	public boolean isShowCheckedMark() {
		return showCheckedMark;
	}

	public void setShowCheckedMark(boolean showCheckedMark) {
		this.showCheckedMark = showCheckedMark;
	}

	public NodeTree getSubTree(String nodeId) {
		if(currentNode!= null && currentNode.getId().equalsIgnoreCase(nodeId)) {
			return this;
		} else {
			if(dependTreeList != null) {
				for(NodeTree nodeTree : dependTreeList) {
					NodeTree childNodeTree = nodeTree.getSubTree(nodeId);
					if(childNodeTree != null) {
						return childNodeTree;
					}
				}
			}
		}
		return null;
	}

	public List<Node> getAllTreeNodes() 
	{
		Set<Node> nodeSet = new HashSet<Node>();
		if(currentNode!=null) nodeSet.add(currentNode);
		if(dependTreeList != null) {
			for(NodeTree nodeTree : dependTreeList) {
				nodeSet.addAll(nodeTree.getAllTreeNodes());
			}
		}
		return new ArrayList<Node>(nodeSet);
	}

	public List<Node> getTerminalNodes() 
	{
		Set<Node> nodeSet = new HashSet<Node>();
		if(dependTreeList != null) {
			for(NodeTree nodeTree : dependTreeList) {
				nodeSet.addAll(nodeTree.getTerminalNodes());
			}
		} else {
			nodeSet.add(currentNode);
		}
		return new ArrayList<Node>(nodeSet);
	}

	public String toString() {
		try {
			return toJSONString();
		} catch (JSONException e) {
			return "";
		}
	}

	public NodeSet clone() {
		return (NodeSet)CloneUtil.clone(this);
	}
	
	public void toJSONArray(JSONArray jsonArray) throws JSONException {
		jsonArray.put(this.toJSONObject());
	}
	
	public JSONObject toJSONObject() throws JSONException {
		JSONObject nodeJSONObject = new JSONObject();
		JSONArray dependentJsonArray = new JSONArray();
		
		if(dependTreeList != null) {
			for(NodeTree nodeTree : dependTreeList) {
				JSONArray jsonArray = new JSONArray();
				JSONObject jsonObject = new JSONObject();
				getDependendJSON(nodeTree, jsonArray);
				jsonObject.put("data", nodeTree.getCurrentNode().getName());
				if(nodeTree.getRelationTypeUp() != null){
					jsonObject.put("relationTypeUp", new JSONArray(nodeTree.getRelationTypeUp()));
				}
				if(nodeTree.getRelationTypeDown() != null){
					jsonObject.put("relationTypeDown", new JSONArray(nodeTree.getRelationTypeDown()));
				}
				
				//--- Add Attributes to JSON
				JSONObject property = new JSONObject();
				property.put("id", nodeTree.getCurrentNode().getId());
				property.put("name", nodeTree.getCurrentNode().getName());
				property.put("reportPoolNames", nodeTree.getCurrentNode().getProperty("reportPoolNames"));
				property.put("renderPoolNames", nodeTree.getCurrentNode().getProperty("renderPoolNames"));
				property.put("reportType", nodeTree.getCurrentNode().getProperty("reportType"));
				property.put("urlHandler", nodeTree.getCurrentNode().getProperty("urlHandler"));
				
				jsonObject.put("parentNode", (nodeTree.getParentNode()!=null)?nodeTree.getParentNode().toJSONObject():"");
				jsonObject.put("includeNode", nodeTree.isIncludeNode());
//				System.out.println();
				if(showCheckedMark) {
					property.put("class", "jstree-checked");
				}
				jsonObject.put("attr", property);
				jsonObject.put("children", jsonArray);
				
				dependentJsonArray.put(jsonObject);
			}
		}
		
		if(this.getRelationTypeUp() != null){
			nodeJSONObject.put("relationTypeUp", new JSONArray(this.getRelationTypeUp()));
		}
		if(this.getRelationTypeDown() != null){
			nodeJSONObject.put("relationTypeDown", new JSONArray(this.getRelationTypeDown()));
		}
		
		if(!isRootNode) {
			nodeJSONObject.put("data", currentNode.getName());
			
			//--- Add Attributes to JSON
			JSONObject property = new JSONObject();
			property.put("id", currentNode.getId());
			property.put("name", currentNode.getName());
			property.put("reportPoolNames", currentNode.getProperty("reportPoolNames"));
			property.put("renderPoolNames", currentNode.getProperty("renderPoolNames"));
			property.put("reportType", currentNode.getProperty("reportType"));
			property.put("urlHandler", currentNode.getProperty("urlHandler"));
			nodeJSONObject.put("parentNode", (parentNode!=null)?parentNode.toJSONObject():"");
			nodeJSONObject.put("includeNode", includeNode);
			
			if(showCheckedMark) {
				property.put("class", "jstree-checked");
			}
			nodeJSONObject.put("attr", property);
			nodeJSONObject.put("children", dependentJsonArray);
		} else {
			nodeJSONObject.put("data", dependentJsonArray);
		}
		
		return nodeJSONObject;
	}
	
	private void getDependendJSON(NodeTree nodeTree, JSONArray jsonArray) throws JSONException {
		if(nodeTree.getDependTreeList() != null && !nodeTree.getDependTreeList().isEmpty()) {
			
			for(NodeTree nodeTree2 : nodeTree.getDependTreeList()) {
				jsonArray.put(nodeTree2.toJSONObject());
			}
		}
	}
	
	public String toJSONString() throws JSONException {
		return toJSONObject().toString();
	}

	public Set<String> getRelationTypeUp() {
		return relationTypeUp;
	}

	public void setRelationTypeUp(Set<String> relationTypeUp) {
		this.relationTypeUp = relationTypeUp;
	}

	public Set<String> getRelationTypeDown() {
		return relationTypeDown;
	}

	public void setRelationTypeDown(Set<String> relationTypeDown) {
		this.relationTypeDown = relationTypeDown;
	}
	
	public void addRelationTypeDown(String relationTypeDown) {
		if(relationTypeDown != null){
			if(this.relationTypeDown == null){
				this.relationTypeDown = new HashSet<String>();
			}
			this.relationTypeDown.add(relationTypeDown);
		}	
	}
	
	public void addRelationTypeUp(String relationTypeUp) {
		if(relationTypeUp != null){
			if(this.relationTypeUp == null){
				this.relationTypeUp = new HashSet<String>();
			}
			this.relationTypeUp.add(relationTypeUp);
		}
	}
}

class RelationImpl implements Relation, Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private String id;
	private RelationType relationType;
	private Node startNode;
	private Node endNode;
	private Map<String, String> propertyMap;
	
	public RelationImpl() {
		this.propertyMap = new HashMap<String, String>();
	}
	
	public RelationImpl(String jsonString) throws JSONException {
		this.propertyMap = new HashMap<String, String>();

		JSONObject newJSONRelation = new JSONObject(jsonString);
		
		setId(newJSONRelation.optString("id"));
		RelationType relationType = new RelationTypeImpl(jsonString);
		setRelationType(relationType);
		if(!newJSONRelation.isNull("startNodeId")) setStartNodeId(newJSONRelation.optString("startNodeId"));
		if(!newJSONRelation.isNull("endNodeId"))  setEndNodeId(newJSONRelation.optString("endNodeId"));
		
		if(!newJSONRelation.isNull("startNode"))  setStartNode(new NodeImpl(newJSONRelation.optString("startNode")));
		if(!newJSONRelation.isNull("endNode"))  setEndNode(new NodeImpl(newJSONRelation.optString("endNode")));

		JSONArray propertyArray = newJSONRelation.optJSONArray("properties");
		if(propertyArray != null) {
			for(int i=0; i<propertyArray.length(); i++) {
				JSONObject property = propertyArray.getJSONObject(i);
				String name = property.optString("name");
				String value = property.optString("value");
				propertyMap.put(name, value);
			}
		}
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public RelationType getRelationType() {
		return relationType;
	}

	public void setRelationType(RelationType relationType) {
		this.relationType = relationType;
	}
	
	public void setRelationTypeId(String relationTypeId) {
		this.relationType = new RelationTypeImpl();
		this.relationType.setId(relationTypeId);
	}
	
	public void setRelationTypeName(String relationTypeName) {
		this.relationType = new RelationTypeImpl();
		this.relationType.setName(relationTypeName);
	}

	public Node getStartNode() {
		return startNode;
	}

	public void setStartNode(Node startNode) {
		this.startNode = startNode;
	}
	
	public void setStartNodeId(String startNodeId) {
		this.startNode = new NodeImpl();
		this.startNode.setId(startNodeId);
	}

	public Node getEndNode() {
		return endNode;
	}
	
	public void setEndNodeId(String endNodeId) {
		this.endNode = new NodeImpl();
		this.endNode.setId(endNodeId);
	}

	public void setEndNode(Node endNode) {
		this.endNode = endNode;
	}

	public Node getOtherNode(Node node) {
		
		if(node.equals(startNode)) {
			return endNode;
		} else if(node.equals(endNode)) {
			return startNode;
		} else {
			throw new IllegalArgumentException("Invalid node given as an input");
		}
	}

	public Node getOtherNode(String nodeId) {
		
		if(nodeId.equals(startNode.getId())) {
			return endNode;
		} else if(nodeId.equals(endNode.getId())) {
			return startNode;
		} else {
			throw new IllegalArgumentException("Invalid node given as an input");
		}
	}
	
	public String getProperty(String key) {
		if(key == null) {
			return null;
		}
		return propertyMap.get(key);
	}

	public void setProperty(String key, String value) {
		if(key == null) {
			throw new IllegalArgumentException("Invalid key=" + key);
		}
		propertyMap.put(key, value);
	}

	public Set<String> getPropertyKeySet() {
		return propertyMap.keySet();
	}

	public void removeProperty(String key) {
		propertyMap.remove(key);
	}
	
	public Map<String, String> getPropertyMap() {
		return propertyMap;
	}

	public void setPropertyMap(Map<String, String> propertyMap) {
		this.propertyMap = propertyMap;
	}

	public boolean equals(Object obj)
	{
		if(this == obj) {
			return true;
		}
		if((obj == null) || (obj.getClass() != this.getClass())) {
			return false;
		}
		Relation anotherRelation = (Relation)obj;
		if(this.id == null || anotherRelation.getId() == null) {
			if(this.getStartNode() != null && anotherRelation.getStartNode() != null && this.getStartNode().equals(anotherRelation.getStartNode()) &&
					this.getRelationType() != null && anotherRelation.getRelationType() != null && this.getRelationType().equals(anotherRelation.getRelationType()) &&
					this.getEndNode() != null && anotherRelation.getEndNode() != null && this.getEndNode().equals(anotherRelation.getEndNode())
					) 
			{
				return true;
			}
			return false;
		}
		
		return (this.id.equals(anotherRelation.getId()));
	}

	public int hashCode()
	{
		int hash = 7;
		hash = 31 * hash + this.startNode.getId().hashCode();
		hash = 31 * hash + this.relationType.getId().hashCode();
		hash = 31 * hash + this.endNode.getId().hashCode();
		return hash;
	}
	
	public JSONObject toJSONObject() throws JSONException {
		JSONObject entity = new JSONObject();
		
		entity.put("id", id);
		entity.put("relationTypeName", relationType.getName());
		entity.put("relationTypeId", relationType.getId());
		if(startNode != null) {
			entity.put("startNodeId", startNode.getId());
			entity.put("startNode", startNode.toJSONObject());
		}
		if(endNode != null) {
			entity.put("endNodeId", endNode.getId());
			entity.put("endNode", endNode.toJSONObject());
		}

		JSONArray propertyArray = new JSONArray();
		for(String key : propertyMap.keySet()) {
			JSONObject property = new JSONObject();
			property.put("name", key);
			property.put("value", propertyMap.get(key));
			propertyArray.put(property);
		}
		entity.put("properties", propertyArray);
		
		return entity;
	}
	
	public String toJSONString() throws JSONException {
		return toJSONObject().toString();
	}
	
	public static <T extends Relation> JSONArray toJSONArray(List<? extends Relation> tList) throws JSONException {
		JSONArray jsonArray = new JSONArray();
		for(Relation entity : tList) {
			jsonArray.put(entity.toJSONObject());
		}
		return jsonArray;
	}

	public static <T extends Relation> String toJSONArrayString(List<? extends Relation> tList) throws JSONException {
		return toJSONArray(tList).toString();
	}
	
	public String toString() {
		try {
			return toJSONString();
		} catch (JSONException e) {
			return null;
		}
	}
	
	public String toFormattedString() {
		StringBuilder sbuf = new StringBuilder();
		
		sbuf.append("(").append((startNode != null ? startNode.getName() : "")).append(")");
		String prefix = "---";
		String suffix = "--->";
		sbuf.append(prefix).append(relationType != null ? relationType.getName() : "").append(suffix);
		sbuf.append("(").append(endNode != null ? endNode.getName() : "").append(")");
		
		return sbuf.toString();
	}
}


