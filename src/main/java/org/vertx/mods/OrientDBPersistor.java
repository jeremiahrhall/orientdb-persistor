package org.vertx.mods;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.graph.gremlin.OCommandGremlin;
import com.orientechnologies.orient.graph.gremlin.OGremlinHelper;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.orient.OrientElement;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;

import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonElement;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Container;

import java.io.IOException;
import java.util.*;

/**
 * Main module class for persisting objects sent on the Vert.x Event Bus with the OrientDB
 * graph database
 *
 * User: jeremiahhall
 * Date: 1/26/14
 * Time: 12:28 PM
 */
public class OrientDBPersistor extends BusModBase implements Handler<Message<JsonObject>> {

  // Message properties
  private static final String ACTION = "action";
  private static final String CLASS = "class";
  private static final String DOCUMENT = "document";
  private static final String CRITERIA = "criteria";

  // Actions - TODO: Review actions in ODB wiki and get them all in here

  // Schema Actions

  // Index Actions
  private static final String CREATE_INDEX = "create_index";
  private static final String GET_INDEX = "get_index";
  private static final String DROP_INDEX = "drop_index";
  private static final String DROP_KEY_INDEX = "drop_key_index";
  private static final String CREATE_KEY_INDEX = "create_key_index";
  private static final String GET_INDEXED_KEYS = "get_indexed_keys";

  // Class Actions (not the legal kind)
  private static final String CREATE_CLASS = "create_class";
  private static final String ALTER_CLASS = "alter_class";
  private static final String DROP_CLASS = "drop_class";

  // Property Actions
  private static final String CREATE_PROPERTY = "create_property";
  private static final String ALTER_PROPERTY = "alter_property";
  private static final String DROP_PROPERTY = "drop_property";

  // Cluster actions - implement v2?

  // Persistence/Document Actions

  // Vertex Actions
  private static final String ADD_VERTEX = "add_vertex";
  private static final String GET_VERTEX = "get_vertex";
  private static final String REMOVE_VERTEX = "remove_vertex";
  private static final String SAVE_VERTEX = "save_vertex";

  // Edge Actions
  private static final String ADD_EDGE = "add_edge";
  private static final String GET_EDGE = "get_edge";
  private static final String REMOVE_EDGE = "remove_edge";
  private static final String SAVE_EDGE = "save_edge";

  // Traversal Actions - implement in v2?

  // Gremlin
  private static final String GREMLIN_TRAVERSAL = "gremlin_traversal";

  // Command Actions - implement in v2?

  // URL prefix
  private static final String REMOTE_PREFIX = "local:";

  protected String address = "vertx.orientdbpersistor";
  protected String host;
  protected String port;
  protected String dbName;
  protected String user;
  protected String password;

  protected OrientGraphFactory factory;
  protected OrientGraph og;
  protected Container c;

  /**
   * start()
   *
   * Perpare the verticle
   *
   * - connect to database
   * - register handlers
   *
   */
  @Override
  public void start() {
    super.start();
    address = getOptionalStringConfig("address", "vertx.orientdbpersistor");
    host = getOptionalStringConfig("host", "localhost");
    port = getOptionalStringConfig("port", "2424");
    dbName = getMandatoryStringConfig("db_name");
    user = getMandatoryStringConfig("username");
    password = getMandatoryStringConfig("password");

    logger.info("Attempting connection");

    factory = new OrientGraphFactory(getConnectionUrl());
    logger.info("got factory");
    try {
      og = factory.getTx();
      initializeGremlin();
      logger.info("Successfully connected to database!");
    } catch (Exception e) {
      logger.error("Error connecting to database", e);
    }


    logger.info("Connected");

    eb.registerHandler(address, this);

    //get connection params for orientdb

    //connect to orientdb

    //register with event bus
  }

  /**
   * initializeGremlin
   *
   * Load Gremlin and run a simple query to initialize it
   *
   * Query time for first query seems very high so, we do one here to preempt that.
   */
  private void initializeGremlin() {
    OGremlinHelper.global().create();
    OCommandRequest req  = og.command(new OCommandGremlin("g.v(1)"));
    req.execute();
  }

  /**
   * getConnectionUrl
   *
   * Creates an OrientDB database URL with the following syntax -
   *
   *    remote:<server>:[<port>]/db-name
   *
   * https://github.com/orientechnologies/orientdb/wiki/Concepts#wiki-Database_URL
   *
   * @return an OrientDB database URL
   */
  private String getConnectionUrl() {
    StringBuilder sb = new StringBuilder();
    // TODO: decided to make the dbs plocal
    sb.append("plocal:");
    /*sb.append(host);

    if (port != null) {
      sb.append(":" + port);
    }*/

    sb.append("/Users/jeremiahhall/Projects/vertx-workspace/orientdbs/" + dbName);

    return sb.toString();
  }


  /**
   * handle(message)
   *
   * handle a message received from the vert.x event bus
   *
   * Mostly a switch dispatch to methods for handling actions
   *
   * @param message
   */
  @Override
  public void handle(Message<JsonObject> message) {

    String action = message.body().getString(ACTION);

    if (action == null) {
      sendError(message, "action must be specified");
      return;
    }

    try {

      switch (action) {
        // vertex actions
        case ADD_VERTEX:
          addVertex(message);
          break;

        case GET_VERTEX:
          getVertex(message);
          break;

        case REMOVE_VERTEX:
          removeVertex(message);
          break;

        case SAVE_VERTEX:
          saveVertex(message);
          break;

        // edge actions
        case ADD_EDGE:
          addEdge(message);
          break;

        case GET_EDGE:
          getEdge(message);
          break;

        case REMOVE_EDGE:
          removeEdge(message);
          break;

        case SAVE_EDGE:
          saveEdge(message);
          break;

        // class actions
        case CREATE_CLASS:
          createClass(message);
          break;

        case ALTER_CLASS:
          alterClass(message);
          break;

        case DROP_CLASS:
          dropClass(message);
          break;

        // property actions
        case CREATE_PROPERTY:
          createProperty(message);
          break;

        case ALTER_PROPERTY:
          alterProperty(message);
          break;

        case DROP_PROPERTY:
          dropProperty(message);
          break;

        // index actions
        case CREATE_INDEX:
          createIndex(message);
          break;

        case GET_INDEX:
          getIndex(message);
          break;

        case DROP_INDEX:
          dropIndex(message);
          break;

        case DROP_KEY_INDEX:
          dropKeyIndex(message);
          break;

        case CREATE_KEY_INDEX:
          createKeyIndex(message);
          break;

        case GET_INDEXED_KEYS:
          getIndexedKeys(message);
          break;

        // gremlin traversal
        case GREMLIN_TRAVERSAL:
          executeGremlinTraversal(message);
          break;

        default:
          sendError(message, "Invalid action: " + action);
      }
    } catch (Exception e) {
      sendError(message, e.getMessage(), e);
    }
  }

  // Execute Gremlin Traversal
  private void executeGremlinTraversal(Message<JsonObject> message) {
    og = factory.getTx();
    String traversalScript = message.body().getString("traversal");
    OCommandRequest req = og.command(new OCommandGremlin(traversalScript));
    Object output = req.execute();

    if (output instanceof Element)
      sendOK(message, new JsonObject(getElementAsMap((Element) output)));

    sendOK(message);
  }

  // Edge Operation Methods

  private void saveEdge(Message<JsonObject> message) {

  }

  private void removeEdge(Message<JsonObject> message) {

  }

  private void getEdge(Message<JsonObject> message) {
    String clazz = message.body().getString(CLASS);
    JsonObject criteria = message.body().getObject(CRITERIA);

    logger.info("Class:" + clazz);
    logger.info("Criteria:" + criteria);
    sendOK(message);
  }

  private void addEdge(Message<JsonObject> message) {
    try {
      JsonObject mBody = message.body();
      String clazz = mBody.getString("class");
      String cluster = mBody.getString("cluster", null);
      JsonObject props = mBody.getObject("properties");
      String inId = mBody.getString("in");
      String outId = mBody.getString("out");
      String label = mBody.getString("label");

      og = factory.getTx();

      Vertex in = og.getVertex(inId);
      Vertex out = og.getVertex(outId);

      Edge newEdge = og.addEdge(null, out, in, label);
      props.getFieldNames().forEach(
        (fieldName) ->
          newEdge.setProperty(fieldName, props.getValue(fieldName))
      );

      og.commit();

      JsonObject reply = new JsonObject(getEdgeAsMap(newEdge, true));

      sendOK(message, reply);
    } catch (Exception e) {
      logger.error(e);
      sendError(message, e.toString());
      og.rollback();
    }
  }

  // Vertex Operation Methods

  private void saveVertex(Message<JsonObject> message) {
    try {
      JsonObject mBody = message.body();
      String id = mBody.getString("id");
      JsonObject props = mBody.getObject("properties");

      og = factory.getTx();

      Vertex v = og.getVertex(id);
      props.getFieldNames().forEach(
        (fieldName) ->
          v.setProperty(fieldName, props.getValue(fieldName))
      );

      og.commit();

      JsonObject reply = new JsonObject(getVertexAsMap(v, true));

      sendOK(message, reply);
    } catch (Exception e) {
      logger.error(e);
      sendError(message, e.toString());
      og.rollback();
    }
  }

  private void removeVertex(Message<JsonObject> message) {
    JsonObject mBody = message.body();
    Map<String, Object> criteria = mBody.getObject("criteria").toMap();

    og = factory.getTx();

    if (criteria.containsKey("id")) {
      Vertex toRemove = og.getVertex(criteria.get("id"));
      toRemove.remove();
    }

    og.commit();

    sendOK(message);
  }

  private void getVertex(Message<JsonObject> message) {
    JsonObject mBody = message.body();
    Map<String, Object> criteria = mBody.getObject("criteria").toMap();

    JsonObject reply = null;

    og = factory.getTx();

    if (criteria.containsKey("id")) {
      Vertex get = og.getVertex(criteria.get("id"));
      reply = new JsonObject(getVertexAsMap(get, true));
    }

    sendOK(message, reply);
  }

  private void addVertex(Message<JsonObject> message) {
    try {
      JsonObject mBody = message.body();
      String clazz = mBody.getString("class");
      String cluster = mBody.getString("cluster", null);
      JsonObject props = mBody.getObject("properties");

      og = factory.getTx();

      Vertex newVertex = og.addVertex(clazz, cluster);
      props.getFieldNames().forEach(
        (fieldName) ->
          newVertex.setProperty(fieldName, props.getValue(fieldName))
      );

      og.commit();

      JsonObject reply = new JsonObject(getVertexAsMap(newVertex, true));

      sendOK(message, reply);
    } catch (Exception e) {
      logger.error(e);
      sendError(message, e.toString());
      og.rollback();
    }
  }

  private Map<String, Object> getElementAsMap(Element element) {
    if (element instanceof Vertex)
      return getVertexAsMap((Vertex) element, true);
    else
      return getEdgeAsMap((Edge) element, true);
  }

  private Map<String, Object> getVertexAsMap(Vertex v, boolean includeEdges) {
    Set<String> propKeys = v.getPropertyKeys();
    Map<String, Object> elMap = new HashMap<>(5);

    elMap.put("id", v.getId().toString());
    elMap.put("class", ((OrientElement) v).getRecord().getSchemaClass().getName());

    Map<String, Object> properties = new HashMap<>(propKeys.size());

    propKeys.forEach(
      (propkey) ->
        properties.put(propkey, v.getProperty(propkey))
    );

    elMap.put("properties", properties);

    if (includeEdges) {
      Iterable<Edge> inEdges = v.getEdges(Direction.IN);

      Map<String, Object> inEdgeMap = new HashMap<>();

      inEdges.forEach(
        (edge) ->
          inEdgeMap.put(edge.getLabel(), getEdgeAsMap(edge, false))
      );

      Iterable<Edge> outEdges = v.getEdges(Direction.OUT);

      Map<String, Object> outEdgeMap = new HashMap<>();

      outEdges.forEach(
        (edge) ->
          outEdgeMap.put(edge.getLabel(), getEdgeAsMap(edge, false))
      );

      elMap.put("in", inEdgeMap);
      elMap.put("out", outEdgeMap);
    }

    return elMap;
  }

  private Map<String, Object> getEdgeAsMap(Edge e, boolean includeVertices) {
    Set<String> propKeys = e.getPropertyKeys();
    Map<String, Object> elMap = new HashMap<>(5);
    elMap.put("id", e.getId().toString());
    elMap.put("class", ((OrientElement) e).getRecord().getSchemaClass().getName());

    Map<String, Object> properties = new HashMap<>(propKeys.size());

    propKeys.forEach(
      (propkey) ->
        properties.put(propkey, e.getProperty(propkey))
    );

    elMap.put("properties", properties);

    if (includeVertices) {
      Vertex out = e.getVertex(Direction.OUT);

      elMap.put("out", getVertexAsMap(out, false));

      Vertex in = e.getVertex(Direction.IN);

      elMap.put("in", getVertexAsMap(in, false));
    }

    return elMap;
  }

  // Index Operation Methods

  private void dropKeyIndex(Message<JsonObject> message) {

  }

  private void dropIndex(Message<JsonObject> message) {

  }

  private void getIndex(Message<JsonObject> message) {

  }

  private void createIndex(Message<JsonObject> message) {

  }

  private void createKeyIndex(Message<JsonObject> message) {

  }

  private void getIndexedKeys(Message<JsonObject> message) {

  }

  // Class Action Methods

  private void dropClass(Message<JsonObject> message) {

  }

  private void alterClass(Message<JsonObject> message) {

  }

  private void createClass(Message<JsonObject> message) {

  }

  // Property Action Methods

  private void createProperty(Message<JsonObject> message) {

  }

  private void alterProperty(Message<JsonObject> message) {

  }

  private void dropProperty(Message<JsonObject> message) {

  }

  /**
   * stop()
   *
   * - close db connection(s)
   *
   */
  @Override
  public void stop() {
    //close connection
    factory.close();
    super.stop();
  }



}
