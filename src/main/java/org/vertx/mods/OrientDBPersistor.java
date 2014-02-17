package org.vertx.mods;

import com.tinkerpop.blueprints.Contains;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;

import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Container;

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
  // private static final String GREMLIN_TRAVERSAL = "gremlin_traversal";

  // Command Actions - implement in v2?

  // URL prefix
  private static final String REMOTE_PREFIX = "remote:";

  protected String address = "vertx.orientdbpersistor";
  protected String host;
  protected String port;
  protected String dbName;
  protected TransactionalGraph graph;
  protected OrientGraph og;
  protected Container c;


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

    sb.append(REMOTE_PREFIX);
    sb.append(host);

    if (port != null) {
      sb.append(":" + port);
    }

    sb.append("/" + dbName);

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

        default:
          sendError(message, "Invalid action: " + action);
      }
    } catch (Exception e) {
      sendError(message, e.getMessage(), e);
    }
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
  }

  private void addEdge(Message<JsonObject> message) {

  }

  // Vertex Operation Methods

  private void saveVertex(Message<JsonObject> message) {

  }

  private void removeVertex(Message<JsonObject> message) {

  }

  private void getVertex(Message<JsonObject> message) {

  }

  private void addVertex(Message<JsonObject> message) {

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

    eb.registerHandler(address, this);

    //get connection params for orientdb

    //connect to orientdb

    //register with event bus
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
    super.stop();
  }



}
