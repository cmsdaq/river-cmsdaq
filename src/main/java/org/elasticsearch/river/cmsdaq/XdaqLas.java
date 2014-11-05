package org.elasticsearch.river.cmsdaq;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;


public class XdaqLas extends DaqRiver implements River {

  protected final String lasURL;
  private volatile Thread thread;

  @SuppressWarnings({"unchecked"})
  @Inject
  public XdaqLas(RiverName riverName, RiverSettings settings, Client client) {
    super(riverName, settings, client);

    //lasURL = XContentMapValues.nodeStringValue(settings.settings().get("lasURL"), "http://dvsrv-c2f36-09-01.cms:9941");
    lasURL = XContentMapValues.nodeStringValue(settings.settings().get("lasURL"), "http://pc-c2e11-18-01.cms:9941");
  }

  @Override
  public void start() {
    logger.info("Start retrieving data from "+lasURL);
    thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "las_slurper").newThread(
      new Slurper(lasURL,this.lasProxy)
    );
    thread.start();
  }

  @Override
  public void close() {
    if (thread != null) {
      thread.interrupt();
    }
    if (this.bulkProcessor != null) {
      this.bulkProcessor.close();
    }
    logger.info("Stopped retrieving data from "+lasURL);
  }


  private class Slurper implements Runnable {
    private final String lasURN;
    private final Proxy lasProxy;
    private final JSONParser parser;
    private final DateFormat dateFormat;

    private Slurper(String lasURL, Proxy lasProxy) {
      this.lasURN = lasURL+"/urn:xdaq-application:service=xmaslas2g";
      this.lasProxy = lasProxy;
      this.parser = new JSONParser();
      this.dateFormat = new SimpleDateFormat("E, MMM dd yyyy HH:mm:ss z");
    }

    @Override
    public void run() {
      HttpURLConnection conn = null;
      InputStream in = null;
      URL url = null;

      try {
        while(true) {
          logger.info("Getting data from "+lasURN);
          try {
            url = new URL( lasURN + "/retrieveCatalog?fmt=plain");
          }
          catch (java.net.MalformedURLException e) {
            logger.error("Bad LAS URL: "+ExceptionUtils.getRootCauseMessage(e));
            continue;
          }
          try {
            if (lasProxy == null) {
              conn = (HttpURLConnection)url.openConnection();
            }
            else {
              conn = (HttpURLConnection)url.openConnection(lasProxy);
            }
            in = conn.getInputStream();
            InputStreamReader isr = new InputStreamReader(in);
            BufferedReader reader = new BufferedReader(isr);
            for (boolean firstLine = true;;firstLine = false) {
              String line = reader.readLine();
              if (line == null)
                break;
              if (firstLine)
                continue;
              slurpFlashList(line);
            }
          }
          catch (IOException e) {
            logger.error("Error retrieving catalog from LAS "+lasURN+": "+ExceptionUtils.getRootCauseMessage(e));
          }
          Thread.sleep(10000);
        }
      }
      catch (java.lang.InterruptedException e)
      {}
    }

    private void slurpFlashList(String flashList) {
      HttpURLConnection conn = null;
      InputStream in = null;
      URL url = null;
      try {
        url = new URL(lasURN+"/retrieveCollection?flash="+flashList+"&fmt=json");
      }
      catch (java.net.MalformedURLException e) {
        logger.error("Bad LAS URL: "+ExceptionUtils.getStackTrace(e));
        return;
      }
      JSONArray array;
      try {
        if (lasProxy == null) {
          conn = (HttpURLConnection)url.openConnection();
        }
        else {
          conn = (HttpURLConnection)url.openConnection(lasProxy);
        }
        in = conn.getInputStream();
        InputStreamReader isr = new InputStreamReader(in);
        BufferedReader reader = new BufferedReader(isr);
        String rawJSON = reader.readLine();
        JSONObject jsonObject = (JSONObject)this.parser.parse(rawJSON);
        JSONObject table = (JSONObject)jsonObject.get("table");
        //System.out.println(table.get("rows"));
        String indexName = "flashlist";
        String typeName = flashList.substring(flashList.lastIndexOf(':') + 1);
        JSONArray rows=(JSONArray)table.get("rows");
        Iterator it = rows.iterator();
        while (it.hasNext())
        {
          JSONObject row = (JSONObject)it.next();
          //System.out.println(row);
          // Construct an ID which assures that all flashlist are unique
          // but are overwritten with the next update
          String id = (String)row.get("hostname");
          if (id != null) {
            Object geoslot = row.get("geoslot");
            if (geoslot != null) {
              id += "_"+geoslot.toString();
              Object io = row.get("io");
              if (io != null)
                id += "_"+io.toString();
            }
          }
          else {
            for (String key : new String[] {"instance","srcId","partitionNumber","context","FMURL"}) {
              Object k = row.get(key);
              if (k != null) {
                id = k.toString();
                break;
              }
            }
          }
          if (id == null)
            logger.error("No id constructed for "+typeName);
          try {
            Date timestamp = this.dateFormat.parse((String)row.get("timestamp"));
            bulkProcessor.add(
              Requests.indexRequest(indexName)
              .type(typeName)
              .id(id)
              .timestamp(String.valueOf(timestamp.getTime()))
              .source(row.toString())
            );
          }
          catch (java.text.ParseException e) {
            logger.error("Failed to parse timestamp "+row.get("timestamp"));
          }
        }
      }
      catch (IOException e) {
        logger.error("Error retrieving flash list '"+flashList+"' from "+lasURN+": "+ExceptionUtils.getStackTrace(e));
      }
      catch (Exception e) {
        logger.error("Could not parse flash list '"+flashList+"' from "+lasURN+": "+ExceptionUtils.getStackTrace(e));
      }
      finally {
        try
        {
          if (in != null)
            in.close();
          if (conn != null)
            conn.disconnect();
        }
        catch (java.io.IOException e)
        {}
      }
    }
  }
}
