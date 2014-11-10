package org.vertx.mods.orientdb.test.integration.java;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.testComplete;

/**
 * Class Description
 * <p/>
 * User: jeremiahhall
 * Date: 1/26/14
 * Time: 1:31 PM
 */
public class PersistorTest extends TestVerticle {

  private EventBus eb;
  private final String TEST_ADDRESS = "test.orientpersistor.primaryDb";


  @Override
  @Before
  public void start() {
    eb = vertx.eventBus();
    JsonObject config = new JsonObject();
    config.putString("address", TEST_ADDRESS);
    config.putString("db_name", System.getProperty("vertx.orient.database", "persistorTest"));
    config.putString("host", System.getProperty("vertx.orient.host", "localhost"));
    config.putString("port", System.getProperty("vertx.orient.port", "2424"));
    // TODO: Get these into system properties outside of this source code?
    String username = System.getProperty("vertx.orient.username", "root");
    String password = System.getProperty("vertx.orient.password", "3F4A0331735C6048F157C7C0F3998E26278D3B08D8F1B586AD263C424726D7C7");
    if (username != null) {
      config.putString("username", username);
      config.putString("password", password);
    }
    config.putBoolean("fake", false);
    container.deployModule(System.getProperty("vertx.modulename"), config, 1, new AsyncResultHandler<String>() {
      public void handle(AsyncResult<String> ar) {
        if (ar.succeeded()) {
          System.out.println("Starting...");
          PersistorTest.super.start();
        } else {
          ar.cause().printStackTrace();
        }
      }
    });
  }

  @Test
  public void testAddVertex() throws Exception {
    HashMap<String, Object> properties = new HashMap<>(2);
    properties.put("name", "Jeremiah");
    properties.put("balance", 2450.50);

    HashMap<String, Object> person = new HashMap<>(4);
    person.put("properties", properties);
    person.put("class", "person");
    person.put("action", "add_vertex");

    JsonObject newVertex = new JsonObject(person);

    eb.send(TEST_ADDRESS, newVertex,
      (Handler<Message<JsonObject>>) (reply) -> {
        System.out.println("reply body:" + reply.body().toString());
        assertEquals("Jeremiah", reply.body().getObject("properties").getString("name"));
        testComplete();
      }
    );
  }

  @Test
  public void testGremlinTraversal() throws Exception {
    HashMap<String, Object> properties = new HashMap<>(2);
    properties.put("name", "Jeremiah");
    properties.put("balance", 3.00);

    HashMap<String, Object> person = new HashMap<>(4);
    person.put("properties", properties);
    person.put("class", "person");
    person.put("action", "add_vertex");

    JsonObject newVertex = new JsonObject(person);

    eb.send(TEST_ADDRESS, newVertex,
      (Handler<Message<JsonObject>>) (reply) -> {
        String repliedId = reply.body().getString("id");
        JsonObject getEdgeMessage = new JsonObject().putString("action", "gremlin_traversal")
          .putString("traversal", "g.v('"+repliedId+"');");

        eb.send(TEST_ADDRESS, getEdgeMessage,
          (Handler<Message<JsonObject>>) (innerreply) -> {
            System.out.println("reply body:" + innerreply.body().toString());
            assertEquals("true", "true");
            testComplete();
          }
        );
      }
    );
  }

  @Test
  public void testRemoveVertex() throws Exception {
    HashMap<String, Object> properties = new HashMap<>(2);
    properties.put("name", "Jeremiah");
    properties.put("balance", 3.00);

    HashMap<String, Object> person = new HashMap<>(4);
    person.put("properties", properties);
    person.put("class", "person");
    person.put("action", "add_vertex");

    JsonObject newVertex = new JsonObject(person);

    eb.send(TEST_ADDRESS, newVertex,
      (Handler<Message<JsonObject>>) (reply) -> {
        String repliedId = reply.body().getString("id");
        HashMap<String, Object> criteria = new HashMap<>(1);
        criteria.put("id", repliedId);

        HashMap<String, Object> find = new HashMap<>(4);
        find.put("criteria", criteria);
        find.put("action", "remove_vertex");

        JsonObject removeVertex = new JsonObject(person);

        eb.send(TEST_ADDRESS, removeVertex,
          (Handler<Message<JsonObject>>) (innerreply) -> {
            System.out.println("reply body:" + innerreply.body().toString());
            assertEquals("ok", innerreply.body().getString("status"));
            testComplete();
          }
        );
      }
    );
  }

  @Test
  public void testGetVertex() throws Exception {
    HashMap<String, Object> properties = new HashMap<>(2);
    properties.put("name", "Jeremiah");
    properties.put("balance", 3.00);

    HashMap<String, Object> person = new HashMap<>(4);
    person.put("properties", properties);
    person.put("class", "person");
    person.put("action", "add_vertex");

    JsonObject newVertex = new JsonObject(person);

    eb.send(TEST_ADDRESS, newVertex,
      (Handler<Message<JsonObject>>) (reply) -> {
        System.out.println("reply body:" + reply.body().toString());
        String repliedId = reply.body().getString("id");
        HashMap<String, Object> criteria = new HashMap<>(1);
        criteria.put("id", repliedId);

        HashMap<String, Object> find = new HashMap<>(4);
        find.put("criteria", criteria);
        find.put("action", "get_vertex");

        JsonObject getVertex = new JsonObject(find);

        eb.send(TEST_ADDRESS, getVertex,
          (Handler<Message<JsonObject>>) (innerreply) -> {
            System.out.println("reply body:" + innerreply.body().toString());
            assertEquals(repliedId, innerreply.body().getString("id"));
            testComplete();
          }
        );
      }
    );
  }

  @Test
  public void testSaveVertex() throws Exception {
    HashMap<String, Object> properties = new HashMap<>(2);
    properties.put("name", "Jeremiah");
    properties.put("balance", 3.00);

    HashMap<String, Object> person = new HashMap<>(4);
    person.put("properties", properties);
    person.put("class", "person");
    person.put("action", "add_vertex");

    JsonObject newVertex = new JsonObject(person);

    eb.send(TEST_ADDRESS, newVertex,
      (Handler<Message<JsonObject>>) (reply) -> {
        System.out.println("reply body:" + reply.body().toString());
        String repliedId = reply.body().getString("id");

        properties.put("name", "jeremiahrhall");

        HashMap<String, Object> save = new HashMap<>(4);
        save.put("properties", properties);
        save.put("id", repliedId);
        save.put("action", "save_vertex");

        JsonObject saveVertex = new JsonObject(save);

        eb.send(TEST_ADDRESS, saveVertex,
          (Handler<Message<JsonObject>>) (innerreply) -> {
            System.out.println("reply body:" + innerreply.body().toString());
            assertEquals("jeremiahrhall",
              innerreply.body().getObject("properties").getString("name"));
            testComplete();
          }
        );
      }
    );
  }

  @Test
  public void testAddEdge() throws Exception {
    HashMap<String, Object> properties = new HashMap<>(2);
    properties.put("name", "Jeremiah");
    properties.put("balance", 3.00);

    HashMap<String, Object> person = new HashMap<>(4);
    person.put("properties", properties);
    person.put("class", "person");
    person.put("action", "add_vertex");

    JsonObject newVertex = new JsonObject(person);

    eb.send(TEST_ADDRESS, newVertex,
      (Handler<Message<JsonObject>>) (reply) -> {
        System.out.println("reply body:" + reply.body().toString());
        String firstPersonId = reply.body().getString("id");

        properties.put("name", "Michael");
        person.put("properties", properties);

        JsonObject secondVertex = new JsonObject(person);

        eb.send(TEST_ADDRESS, newVertex,
          (Handler<Message<JsonObject>>) (innerreply) -> {
            String secondPersonId = innerreply.body().getString("id");

            HashMap<String, Object> ep = new HashMap<>();
            ep.put("duration", "6 months");

            HashMap<String, Object> newEdge = new HashMap<>();
            newEdge.put("action", "add_edge");
            newEdge.put("label", "works with");
            newEdge.put("class", "coworkers");
            newEdge.put("in", firstPersonId);
            newEdge.put("out", secondPersonId);
            newEdge.put("properties", ep);

            JsonObject ne = new JsonObject(newEdge);

            eb.send(TEST_ADDRESS, ne,
              (Handler<Message<JsonObject>>) (edgereply) -> {
                System.out.println("reply body:" + edgereply.body().toString());
                assertEquals("6 months",
                  edgereply.body().getObject("properties").getString("duration"));
                testComplete();
              }
            );
          }
        );
      }
    );
  }

}
