package org.elasticsearch.river.cmsdaq;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.SocketAddress;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;


public class Switches extends DaqRiver implements River {

  protected final String switchUrl;
  private volatile Thread thread;

  @SuppressWarnings({"unchecked"})
  @Inject
  public Switches(RiverName riverName, RiverSettings settings, Client client) {
    super(riverName, settings, client);

    switchUrl = XContentMapValues.nodeStringValue(settings.settings().get("switchesUrl"), "http://cmsusr1.cms:19343/switches-es-json.jsp");
  }

  @Override
  public void start() {
    logger.info("Start retrieving data from "+switchUrl);
    thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "switches_slurper").newThread(
      new Slurper(switchUrl,this.lasProxy,this.ttl)
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
    logger.info("Stopped retrieving data from "+switchUrl);
  }


  private class Slurper implements Runnable {
    private final String switchUrl;
    private final Proxy proxy;
    private final JSONParser parser;
    private final DateFormat dateFormat;
    private long ttl;

    private Slurper(String switchUrl, Proxy proxy, long ttl) {
      this.switchUrl = switchUrl;
      this.proxy = proxy;
      this.parser = new JSONParser();
      this.dateFormat = new SimpleDateFormat("E, dd MMM yyyy HH:mm:ss z");
      this.ttl = ttl;
    }

    @Override
    public void run() {
      HttpURLConnection conn = null;
      InputStream in = null;
      URL url = null;
      try {
        url = new URL(switchUrl);
      }
      catch (java.net.MalformedURLException e) {
        logger.error("Bad switch URL: "+ExceptionUtils.getStackTrace(e));
        return;
      }

      try {
        while(true) {
          logger.info("Getting data from "+switchUrl);
          try {
            if (proxy == null) {
              conn = (HttpURLConnection)url.openConnection();
            }
            else {
              conn = (HttpURLConnection)url.openConnection(proxy);
            }
            in = conn.getInputStream();
            InputStreamReader isr = new InputStreamReader(in);
            BufferedReader reader = new BufferedReader(isr);
            String rawJSON = reader.readLine();
            JSONObject jsonObject = (JSONObject)this.parser.parse(rawJSON);
            JSONObject table = (JSONObject)jsonObject.get("table");
            JSONObject properties = (JSONObject)table.get("properties");
            //System.out.println(table.get("rows"));
            String indexName = (String)properties.get("Name");
            String timestamp = null;
            try {
              Date lastUpdate = this.dateFormat.parse((String)properties.get("LastUpdate"));
              timestamp = String.valueOf(lastUpdate.getTime());
            }
            catch (java.text.ParseException e) {
              logger.error("Failed to parse timestamp "+properties.get("LastUpdate"));
            }

            JSONArray rows=(JSONArray)table.get("rows");
            Iterator it = rows.iterator();
            while (it.hasNext())
            {
              JSONObject row = (JSONObject)it.next();
              //System.out.println(row);
              String typeName = (String)row.get("switch");
              String id = (String)row.get("port");
              bulkProcessor.add(
              Requests.indexRequest(indexName)
              .type(typeName)
              .id(id)
              .timestamp(timestamp)
              .ttl(ttl)
              .source(row.toString())
              );
            }
          }
          catch (IOException e) {
            logger.error("Error retrieving switch information from "+switchUrl+": "+ExceptionUtils.getStackTrace(e));
          }
          catch (Exception e) {
            logger.error("Could not parse JSON from "+switchUrl+": "+ExceptionUtils.getStackTrace(e));
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
          Thread.sleep(30000);
        }
      }
      catch (java.lang.InterruptedException e)
      {}
    }
  }
}
