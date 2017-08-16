package org.apache.zeppelin.beam;

import com.ebay.dss.beam_sql_demo.BeamRecord;
import com.ebay.dss.beam_sql_demo.SqlResultFormatter;
import java.sql.Types;
import java.util.Arrays;
import java.util.Properties;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.BeamSqlCli;
import org.apache.beam.sdk.extensions.sql.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.schema.BeamRecordSqlType;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.zeppelin.beam.sqlcli.RheosSojournerTable;
import org.apache.zeppelin.beam.sqlcli.ToraMySQLTable;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;

public class BeamSqlInterpreter extends Interpreter {
  private BeamSqlEnv env = new BeamSqlEnv();
  private BeamSqlCli cli = new BeamSqlCli();

  public BeamSqlInterpreter(Properties property) {
    super(property);
  }

  @Override
  public void cancel(InterpreterContext arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void close() {
    // TODO Auto-generated method stub

  }

  @Override
  public FormType getFormType() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getProgress(InterpreterContext arg0) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public InterpreterResult interpret(String query, InterpreterContext arg1) {
    if (query.toUpperCase().replaceAll("  *", " ").equalsIgnoreCase("SHOW TABLES;")) {
      System.out.println("show tables...");
      return listTables();
    } else if (query.toUpperCase().startsWith("EXPLAIN")) {
      System.out.println("explain...");
      explainQuery(query.replaceFirst("explain", "").replace(";", ""));
    } else {
      if (query.toUpperCase().startsWith("INSERT")) {
        System.out.println("inserting...");
        executeQuery(query.replace(";", ""), true);
      } else {
        System.out.println("query...");
        executeQuery(query.replace(";", ""), false);
      }
    }
    return null;
  }
  
  public InterpreterResult listTables(){
    StringBuffer sb = new StringBuffer();
    sb.append(String.format("| %-40s |", "TABLE_NAME")).append("\n");
//    for(String t : env.listAllTables()){
//      sb.append(String.format("| %-40s |", t)).append("\n");
//    }
//    System.out.println(sb.toString());
    return new InterpreterResult(Code.SUCCESS, sb.toString());
  }
  
  public InterpreterResult explainQuery(String query){
    String explain = null;
    try {
      explain = cli.explainQuery(query, env);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return new InterpreterResult(Code.SUCCESS, explain);
  }
  
  public void executeQuery(String query, boolean backendMode){
    PipelineOptions options = PipelineOptionsFactory.fromArgs(new String[]{}).as(PipelineOptions.class);
    
    Pipeline p = Pipeline.create(options);
    PCollection<BeamRecord> result = cli.compilePipeline(query, p, env);
    if(!backendMode){
      result.apply("log_result", new SqlResultFormatter());
    }else{
      System.out.println("query submitted...");
    }
    
    pr = p.run();
  }

  @Override
  public void open() {
    ToraMySQLTable lkpTable = new ToraMySQLTable(
        BeamRecordSqlType.create(Arrays.asList("PAGE_ID", "PAGE_GROUP")
            , Arrays.asList(Types.INTEGER, Types.VARCHAR))
        , "mydb02.vip.arch.ebay.com", 3306
        , "toramysql", "toramysql"
        , "toramysql", "DW_SOJ_LKP_PAGE");
    
    RheosSojournerTable sojEventTable = RheosSojournerTable.create(
        "rheos-bh-stg-agg-kfk-1.slc01.dev.ebayc3.com:9092,rheos-bh-stg-agg-kfk-2.slc01.dev.ebayc3.com:9092,rheos-bh-stg-agg-kfk-3.slc01.dev.ebayc3.com:9092,rheos-bh-stg-agg-kfk-4.slc01.dev.ebayc3.com:9092,rheos-bh-stg-agg-kfk-5.slc01.dev.ebayc3.com:9092"
        , "torarheos-sjoevent"
        , "d4c96f37-3e9d-4b9f-b14b-763ebcc59cd8"
        , "behavior.pulsar.sojevent.total");
    
    ToraMySQLTable summaryTable = new ToraMySQLTable(
        BeamRecordSqlType.create(Arrays.asList("PAGE_GROUP", "EVENT_TIMESTAMP", "EVENT_COUNT")
            , Arrays.asList(Types.VARCHAR, Types.TIMESTAMP, Types.BIGINT))
        , "mydb02.vip.arch.ebay.com", 3306
        , "toramysql", "toramysql"
        , "toramysql", "SOJ_EVENT_SUMMARY");
    
    env.registerTable("SOJ_PAGE_LKP", lkpTable);
    env.registerTable("SOJ_EVENTS", sojEventTable);
    env.registerTable("SOJ_EVENT_SUMMARY", summaryTable);
  }

}
