package com.orange.airelealim;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.orange.airelealim.modelisation.*;

import com.orange.airelealim.technicalservices.CassandraConnector;

import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.cassandra.CassandraSQLContext;

//import org.apache.logging.log4j.core.LoggerContext;
//import org.apache.logging.log4j.core.StatusPrinter;



import scala.collection.IndexedSeq;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.ColumnRef;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.writer.RowWriter;
import com.datastax.spark.connector.writer.RowWriterFactory;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ReaderWriter implements Serializable
{    
	private static final long serialVersionUID = 1L;
	private static String BOTT_REQUEST="BOTT.sql";
	
	public static void main(String[] args) throws Exception {
		
		final Logger logger = LoggerFactory.getLogger(ReaderWriter.class);	
		
		float maxLineNumberToInsert = 1;
		String tableToWrite = null;
		if (args.length == 2) {
		    try {
		    	maxLineNumberToInsert = Float.parseFloat(args[0]);
		    	logger.info("insertion par paquets d'environ {} lignes",maxLineNumberToInsert);
		    	
		    } catch (NumberFormatException e) {
		    	logger.error("Argument " + args[0] + " must be a float.");
		        System.exit(1);
		    }
		    try {
		    	tableToWrite = args[1];
		    	if(tableToWrite.equalsIgnoreCase("bott")||tableToWrite.equalsIgnoreCase("troncons_cables")){
		    		logger.info("table à écraser: {}",tableToWrite);
		    	}
		    	else{
		    		throw new Exception();
		    	}
		    	
		    } catch (Exception e) {
		    	logger.error("Argument " + args[1] + " cannot be assigned to String or table to override does not match available values: [bott, troncons_cables]");
		        System.exit(1);
		    }
		}
		else{
	    	logger.error("Usage: airelealim <max number of lines to insert> <name of table to override: [bott,troncons_cables]>");
	        System.exit(1);
		}
		

		//initialisation contexte Spark SQL
		SparkConf sparkConf = new SparkConf(true)
			.setAppName("ReadFromHive")
			.set("spark.cassandra.connection.host", "dvbdfcas11.rouen.francetelecom.fr")
			.set("spark.cassandra.connection.port", "9042")
			.set("spark.cassandra.auth.username", "a_app_di3_cass")
			.set("spark.cassandra.auth.password", "a_app_di3_cass")
			//.set("spark.cassandra.output.batch.size.bytes", "512");
		 	.set("spark.cassandra.output.batch.size.bytes", "5120")
		 	.set("spark.cassandra.output.throughput_mb_per_sec", "4");
		 	//.set("spark.cassandra.output.ignoreNulls", "true");
		 	//.set("spark.cassandra.output.ifNotExists", "true");
		 	//.set("spark.cassandra.output.batch.grouping.buffer.size","10000");
		
		SparkContext sparkContext = new SparkContext(sparkConf);
        HiveContext hiveContext = new HiveContext(sparkContext);
        
        
		//initialisation session cassandra avec le client datastax
        CassandraConnector cassandra = new CassandraConnector("dvbdfcas11.rouen.francetelecom.fr");
		cassandra.connect();
		Session sessionCassandra=cassandra.getSession();
		
        if(tableToWrite.equalsIgnoreCase("bott")){
    		DataFrame bott=getBOTTData(hiveContext);
    		
    		//logger.debug("avant distinct");
    		//System.out.println("avant distinct");
    		//bott=bott.distinct();
    		//logger.debug("après distinct");
    		//System.out.println("apres distinct");
    		
    		truncateCassandraBOTT(sessionCassandra);
    		
    		if (maxLineNumberToInsert==0){
    			loadBOTTFromHiveToCassandra(bott, hiveContext);
    		}
    		else{
    			//ajout d'une colonne de nombres aléatoires afin de sélectionner le nombre de lignes souhaité
    			bott=bott.withColumn("ID", org.apache.spark.sql.functions.rand());
    			float inferior_step = 0;
    			float superior_step=0;
    			float bottLineCount=bott.count();
    			float step=bottLineCount/maxLineNumberToInsert;
    			step=1/step;
    			superior_step=step;
    			DataFrame subBott;
    			//while (superior_step<1.0){
    				System.out.println("interval: "+ inferior_step+" to "+ superior_step);
    				subBott=bott.where(bott.col("ID").geq(inferior_step))
    						.where(bott.col("ID").lt(superior_step));
    				//System.out.println("nombre de lignes dans subBott:"+ subBott.count());
    				loadBOTTFromHiveToCassandra(subBott, hiveContext);	
    				//inferior_step=superior_step;
    				//superior_step=superior_step+step;
    			//}
    		}
    			
    		//verifications
    		//countBOTTCassandra(sessionCassandra);
    		//queryBOTT(sessionCassandra);
        }
        else if (tableToWrite.equalsIgnoreCase("troncons_cables")){
        	DataFrame distinctTronconsCables = selectDistinctTronconsCablesFromHive(hiveContext); 
        	truncateCassandraTronconsCables(sessionCassandra);
        	
        	if (maxLineNumberToInsert==0){
        		loadTronconsCablesIntoCassandra(distinctTronconsCables,hiveContext);
        	}
        	else{
        		distinctTronconsCables=distinctTronconsCables.withColumn("ID", org.apache.spark.sql.functions.rand());
        		float inferior_step = 0;
        		float superior_step=0;
        		float tronconsCablesLineCount=distinctTronconsCables.count();
        		float step=tronconsCablesLineCount/maxLineNumberToInsert;
        		step=1/step;
        		superior_step=step;
        		DataFrame subTronconsCables;
    		
        		//a decommenter pour faire de l'insertion par paquets
        		//while (superior_step<1.0){
        			System.out.println("interval: "+ inferior_step+" to "+ superior_step);
        			subTronconsCables=distinctTronconsCables.where(distinctTronconsCables.col("ID").geq(inferior_step))
        				.where(distinctTronconsCables.col("ID").lt(superior_step));
        			//System.out.println("nombre de lignes dans subBott:"+ subBott.count());
        			//loadTronconsCablesIntoCassandra_InsertMethod(sessionCassandra,distinctCableTroncon);
        			loadTronconsCablesIntoCassandra(subTronconsCables,hiveContext);
        			//inferior_step=superior_step;
        			//superior_step=superior_step+step;
        		//}
        	}
    		//verifications
    		//countCableTronconCassandra(sessionCassandra);
        }
        else{
	    	logger.error("{} does not match available values: [bott, troncons_cables]",tableToWrite);
	        System.exit(1);
        }
        
		cassandra.close();	
		
		
	}
	
	private static void truncateCassandraTronconsCables(Session sessionCassandra) {
		sessionCassandra.execute(
				"truncate z_app_di3_cass_keyspace.troncons_cables;"
				);		
	}
	
	private static void truncateCassandraBOTT(Session sessionCassandra) {
		sessionCassandra.execute(
				"truncate z_app_di3_cass_keyspace.bil_occup_totale_troncon;"
				);		
	}
	
	private static void countCableTronconCassandra(Session sessionCassandra) {
		ResultSet results = sessionCassandra.execute(
				"select count(*) from z_app_di3_cass_keyspace.troncons_cables;"
				);		
		for (com.datastax.driver.core.Row row : results) {
			System.out.println("nb lignes troncons_cables:" + row);
		}
	}

	private static void countBOTTCassandra(Session sessionCassandra) {
		ResultSet results = sessionCassandra.execute(
				"select count(*) from z_app_di3_cass_keyspace.bil_occup_totale_troncon;"
				);		
		for (com.datastax.driver.core.Row row : results) {
		    System.out.println("nb lignes bil_occup_totale_troncon:" + row);
		}
	}
	
	static public DataFrame getBOTTData(HiveContext hiveContext){
		System.out.println("debut getBOTTData()");
		DataFrame df;
		try {
			//String SQLrequest = readFile(System.getProperty(BOTT_REQUEST), StandardCharsets.UTF_8);
			String SQLrequest = readFile(BOTT_REQUEST, StandardCharsets.UTF_8);
			df = hiveContext.sql(SQLrequest);
			System.out.println("nombre de lignes HIVE BOTT:"+ df.count());
			return df;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}

	}
	
	static public DataFrame selectDistinctTronconsCablesFromHive(HiveContext hiveContext){
		System.out.println("debut selectDistinctTronconsCablesFromHive()");
		DataFrame df = hiveContext.sql("select distinct nomcable,cdtron,agla_troncon,sitea_troncon,pointa_troncon,aglb_troncon,siteb_troncon,pointb_troncon FROM z_app_di3_hive_socle.diana_cable limit 15000000");
		System.out.println("nombre de lignes HIVE DianaCable:"+ df.count());
		//df=df.distinct();
		//System.out.println("nombre de lignes uniques HIVE DianaCable:"+ df.count());
        return df;
	}
	
	static public DataFrame selectDianaCableFromHive(HiveContext hiveContext){
		System.out.println("debut selectDianaCableFromHive()");
		DataFrame df = hiveContext.sql("select distinct cdtron,nomcable,cdmephy,idelem_clp,typ_clp,cdcanal1,cdcanal2,agla_clp,sitea_clp,pointa_clp,aglb_clp,siteb_clp,pointb_clp FROM z_app_di3_hive_socle.diana_cable limit 15000000");
		System.out.println("nombre de lignes HIVE DianaCable:"+ df.count());
		//df=df.distinct();
		//System.out.println("nombre de lignes uniques HIVE DianaCable:"+ df.count());
        return df;
	}
	
	static public DataFrame selectDianaOccupationFromHive(HiveContext hiveContext){
		System.out.println("debut selectDianaOccupationFromHive()");
		DataFrame df = hiveContext.sql("select distinct idsupport,idvrssupte,niv,voie FROM z_app_di3_hive_socle.diana_occupation limit 15000000");
		System.out.println("nombre de lignes HIVE DianaOccupation:"+ df.count());
		//df=df.distinct();
		//System.out.println("nombre de lignes uniques HIVE DianaOccupation:"+ df.count());
        return df;
	}
	
	static public DataFrame selectDianaLienCalculImpactFromHive(HiveContext hiveContext){
		System.out.println("debut selectDianaLienCalculImpactFromHive()");
		DataFrame df = hiveContext.sql("select distinct idvrsass,cdfonctint,noserieint,typs_dico_a,client_dico_a,typs_dico_b,client_dico_b,cat,typ,typssres,nomdom,nomprod,idtopo,nomcttopo,ind_protection,etat_complet,entsup,typeqptla,nom_nat_a,typeqptlb,nom_nat_b,nomtit,noma,nomb,refarticle,libarticle,typ_client,gtr,etat_comm FROM z_app_di3_hive_socle.diana_lien_calcul_impact limit 1500000");
		System.out.println("nombre de lignes HIVE DianaLienCalculImpact:"+ df.count());
		//df=df.distinct();
		//System.out.println("nombre de lignes uniques HIVE DianaLienCalculImpact:"+ df.count());
        return df;
	}
	
	static public DataFrame selectDianaLienIdGlobalFromHive(HiveContext hiveContext){
		System.out.println("debut selectDianaLienIdGlobalFromHive()");
		DataFrame df = hiveContext.sql("select distinct idglobal,famille,nomeqptla,nomeqptlb FROM z_app_di3_hive_socle.diana_lien_idglobal limit 1500000");
		System.out.println("nombre de lignes HIVE DianaLienIdGlobal:"+ df.count());
		//df=df.distinct();
		//System.out.println("nombre de lignes uniques HIVE DianaLienIdGlobal:"+ df.count());
        return df;
	}
	
	static public DataFrame selectRcglobalFromHive(HiveContext hiveContext){
		System.out.println("debut selectRcglobalFromHive()");
		DataFrame df = hiveContext.sql("select distinct cat,nd,nomeqptl_acl,nomeqptl_src FROM z_app_di3_hive_socle.ref_edr_dsl_rcglobal");
		System.out.println("nombre de lignes HIVE Rcglobal:"+ df.count());
		//df=df.distinct();
		//System.out.println("nombre de lignes uniques HIVE Rcglobal:"+ df.count());
        return df;
	}
	
	static public DataFrame selectClipcomFromHive(HiveContext hiveContext){
		System.out.println("debut selectClipcomFromHive()");
		DataFrame df = hiveContext.sql("select distinct numpres,gtr FROM z_app_di3_hive_socle.clipcom limit 1500000");
		System.out.println("nombre de lignes HIVE clipcom:"+ df.count());
		//df=df.distinct();
		//System.out.println("nombre de lignes uniques HIVE clipcom:"+ df.count());
        return df;
	}
	
	
	static public void selectCassandra(CassandraSQLContext cassandraContext){
		System.out.println("debut selectCassandra()");
		//cassandraContext.setKeyspace("z_app_di3_cass_keyspace");
		//System.out.println("setKeyspace done");
        //DataFrame myTroncons = cassandraContext.sql("SELECT * FROM troncons_cables WHERE cable=3KED004 AND troncon=W306046");
		//DataFrame myTroncons = cassandraContext.sql("SELECT * FROM z_app_di3_cass_keyspace.troncons_cables WHERE cable='3KED004' AND troncon='W306046'");
		DataFrame myTroncons = cassandraContext.sql("SELECT * FROM z_app_di3_cass_keyspace.troncons_cables");
		System.out.println("nombre de lignes Cassandra:"+ myTroncons.count());
	}
    
	public static void loadTronconsCablesIntoCassandra_InsertMethod(Session sessionCassandra, DataFrame df) {
		System.out.println("debut loadTronconsCablesIntoCassandra_InsertMethod()");
		List<Row> troncons_cables = df.collectAsList();
		
		PreparedStatement statement = sessionCassandra.prepare(
				"insert into z_app_di3_cass_keyspace.troncons_cables " +
				"(cable, troncon, agla_troncon, sitea_troncon, pointa_troncon, aglb_troncon, siteb_troncon, pointb_troncon)" +
				" VALUES (?,?,?,?,?,?,?,?);"
				);
		BoundStatement boundStatement = new BoundStatement(statement);
		
		for (Row cable_troncon : troncons_cables) {
			ResultSet result = sessionCassandra.execute(boundStatement.bind(
					cable_troncon.get(0).toString().trim(),cable_troncon.get(1).toString().trim(),
					cable_troncon.get(2).toString().trim(),cable_troncon.get(3).toString().trim(),
					cable_troncon.get(4).toString().trim(),cable_troncon.get(5).toString().trim(),
					cable_troncon.get(6).toString().trim(),cable_troncon.get(7).toString().trim())
					);		
		}
		System.out.println("fin loadTronconsCablesIntoCassandra_InsertMethod()");
	}
	
	public static void loadTronconsCablesIntoCassandra(DataFrame df, HiveContext hiveContext) {
		System.out.println("debut loadTronconsCablesIntoCassandra()");
		
		//df.insertIntoJDBC(url, table, overwrite)
		//CassandraJavaUtil.javaFunctions(df).
		
		/*//autre methode d'insertion qui necessite d'avoir des droits sur des tables systeme
		Map<String, String> insertionOptions = new HashMap<String, String>();
		insertionOptions.put("table","troncons_cables");
		insertionOptions.put("keyspace","z_app_di3_cass_keyspace");
		df.write().format("org.apache.spark.sql.cassandra").options(insertionOptions).save();
		*/
		

		
		JavaRDD dataRDD = df.toJavaRDD().map(new Function<Row,TronconCable>(){
			public TronconCable call(Row row) throws Exception {
			       TronconCable tronconCable = new TronconCable();
			       tronconCable.setCable(row.getString(0).trim());
			       tronconCable.setTroncon(row.getString(1).trim());
			       tronconCable.setAgla_troncon(row.getString(2).trim());
			       tronconCable.setSitea_troncon(row.getString(3).trim());
			       tronconCable.setPointa_troncon(row.getString(4).trim());
			       tronconCable.setAglb_troncon(row.getString(5).trim());
			       tronconCable.setSiteb_troncon(row.getString(6).trim());
			       tronconCable.setPointb_troncon(row.getString(7).trim());
			       return tronconCable;
			}
		});
		
		
		//sortie de la dataframe dans un fichier
		/*
		DataFrame df2 = hiveContext.createDataFrame(dataRDD, TronconCable.class);
		df2.write()
			//.coalesce(1)
			.format("com.databricks.spark.csv")
			.option("header", "false")
			.option("delimiter", ";")
			.save("/tmp/a_app_di3/cables_troncons");
		*/
		
		System.out.println("avant injection dans cassandra");
		CassandraJavaUtil.javaFunctions(dataRDD).writerBuilder("z_app_di3_cass_keyspace", "troncons_cables", new TronconsCablesWriterFactory()).saveToCassandra();
		System.out.println("injection dans cassandra terminée");
	}   
	   
	  public static void loadBOTTFromHiveToCassandra(DataFrame df, HiveContext hiveContext){
			System.out.println("debut loadBOTTFromHiveToCassandra()");
			
			//System.out.println("loadBOTTFromHiveToCassandra() - contenu des lignes du df:"+ df.showString(50, false));
			
			JavaRDD<BilOccupTotaleTroncon> dataRDD = df.toJavaRDD().map(new Function<Row,BilOccupTotaleTroncon>(){
				private static final long serialVersionUID = 1L;
				public BilOccupTotaleTroncon call(Row row) throws Exception{
					BilOccupTotaleTroncon bilOccupTotaleTroncon = new BilOccupTotaleTroncon();
					bilOccupTotaleTroncon.setNom_nat_a(row.getString(0));
					bilOccupTotaleTroncon.setNom_nat_b(row.getString(1));
					bilOccupTotaleTroncon.setNomprod(row.getString(2));
					bilOccupTotaleTroncon.setNom_tit(row.getString(3));
					bilOccupTotaleTroncon.setNom_a(row.getString(4));
					bilOccupTotaleTroncon.setNom_b(row.getString(5));
					bilOccupTotaleTroncon.setGtr(row.getString(6));
					bilOccupTotaleTroncon.setTroncon(row.getString(7));
					bilOccupTotaleTroncon.setCable(row.getString(8));
					bilOccupTotaleTroncon.setPaire(row.getString(9));
					bilOccupTotaleTroncon.setNiv_occ(Integer.toString(row.getInt(10)));
					bilOccupTotaleTroncon.setVoie(Integer.toString(row.getInt(11)));
					bilOccupTotaleTroncon.setLien(row.getString(12));
					bilOccupTotaleTroncon.setTopologie(row.getString(13));
					bilOccupTotaleTroncon.setTyp_topologie(row.getString(14));
					bilOccupTotaleTroncon.setProtection(row.getString(15));
					bilOccupTotaleTroncon.setDomaine(row.getString(16));
					bilOccupTotaleTroncon.setEtat(row.getString(17));
					bilOccupTotaleTroncon.setEntite_sup(row.getString(18));
					bilOccupTotaleTroncon.setTyp_eqptl_a(row.getString(19));
					bilOccupTotaleTroncon.setTyp_eqptl_b(row.getString(20));
					bilOccupTotaleTroncon.setAtm(row.getString(21));
					bilOccupTotaleTroncon.setCommut(row.getString(22));
					bilOccupTotaleTroncon.setOinis(row.getString(23));
					bilOccupTotaleTroncon.setIp(row.getString(24));
					bilOccupTotaleTroncon.setLl_bd(row.getString(25));
					bilOccupTotaleTroncon.setMobile(row.getString(26));
					bilOccupTotaleTroncon.setObs(row.getString(27));
					bilOccupTotaleTroncon.setOwf(row.getString(28));
					bilOccupTotaleTroncon.setRtge(row.getString(29));
					bilOccupTotaleTroncon.setTrans(row.getString(30));
					bilOccupTotaleTroncon.setXdsl(row.getString(31));
					bilOccupTotaleTroncon.setAutres(row.getString(32));
					bilOccupTotaleTroncon.setClp(row.getString(33));
					bilOccupTotaleTroncon.setTyp_clp(row.getString(34));
					bilOccupTotaleTroncon.setCat(row.getString(35));
					bilOccupTotaleTroncon.setCdfonctint(row.getString(36));
					bilOccupTotaleTroncon.setTyp_lien(row.getString(37));
					bilOccupTotaleTroncon.setTyp_reseau_lien(row.getString(38));
					bilOccupTotaleTroncon.setTyp_dico_a(row.getString(39));
					bilOccupTotaleTroncon.setTyp_dico_b(row.getString(40));
					bilOccupTotaleTroncon.setClient_dico_a(row.getString(41));
					bilOccupTotaleTroncon.setClient_dico_b(row.getString(42));
					bilOccupTotaleTroncon.setRef_article(row.getString(43));
					bilOccupTotaleTroncon.setLib_article(row.getString(44));
					bilOccupTotaleTroncon.setTyp_client(row.getString(45));
					bilOccupTotaleTroncon.setEtat_comm(row.getString(46));
					bilOccupTotaleTroncon.setIdelem_clp(row.getString(47));
				    return bilOccupTotaleTroncon;
				}
			});
			
			//org.apache.hadoop.io.compress.GzipCodec codec = new org.apache.hadoop.io.compress.GzipCodec();
			//DataFrame df2 = hiveContext.createDataFrame(dataRDD, BilOccupTotaleTroncon.class);
			
			//sortie de la dataframe dans un fichier
			/*System.out.println("avant injection de BilOccupTotaleTroncon dans le fichier de sortie");
			df2.write()
				.format("com.databricks.spark.csv")
				.option("header", "false")
				.option("delimiter", "|")
				.save("/tmp/a_app_di3/bott");
			*/
			
			System.out.println("avant injection de BilOccupTotaleTroncon dans cassandra");
			CassandraJavaUtil.javaFunctions(dataRDD).writerBuilder("z_app_di3_cass_keyspace", "bil_occup_totale_troncon", new BilOccupTotaleTronconWriterFactory()).saveToCassandra();
			System.out.println("injection dans cassandra de BilOccupTotaleTroncon terminee");
	  }
	  
	  public static void queryBOTT(Session sessionCassandra){
		  System.out.println("debut queryBOTT()");
			ResultSet results = sessionCassandra.execute("SELECT * FROM z_app_di3_cass_keyspace.bil_occup_totale_troncon limit 5 ALLOW FILTERING;"
			        );
			
			for (com.datastax.driver.core.Row row : results) {
			    System.out.println(row);
			}
				System.out.println();
		}
	  
	  static String readFile(String path, Charset encoding) 
			  throws IOException 
			{
			  byte[] encoded = Files.readAllBytes(Paths.get(path));
			  return new String(encoded, encoding);
			}
}