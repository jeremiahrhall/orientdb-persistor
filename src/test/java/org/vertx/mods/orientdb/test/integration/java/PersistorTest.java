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

  @Override
  @Before
  public void start() {
    eb = vertx.eventBus();
    JsonObject config = new JsonObject();
    config.putString("address", "test.orientpersistor");
    config.putString("db_name", System.getProperty("vertx.orient.database", "persistorTest"));
    config.putString("host", System.getProperty("vertx.orient.host", "localhost"));
    config.putString("port", System.getProperty("vertx.orient.port", "2424"));
    // TODO: Get these into system properties outside of this source code?
    String username = System.getProperty("vertx.orient.username", "root");
    String password = System.getProperty("vertx.orient.password", "AD27EAF821246F758C7D50BFF9EBC95FE4712EDB327CA98D8C72DF8FECB65568");
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

    JsonObject getEdgeMessage = new JsonObject().putString("action", "add_vertex")
        .putString("class", "edgeClass").putObject("criteria", new JsonObject());

    eb.send("test.orientpersistor", getEdgeMessage, new Handler<Message<JsonObject>>() {
      public void handle(Message<JsonObject> reply) {
        System.out.println("reply body:" + reply.body().toString());
        assertEquals("true", "true");
        testComplete();
      }
    });
  }

  /*@Test
  public void testPersistor() throws Exception {

    JsonObject getEdgeMessage = new JsonObject().putString("action", "get_edge")
        .putString("class", "edgeClass").putObject("criteria", new JsonObject());

    eb.send("test.orientpersistor", getEdgeMessage, new Handler<Message<JsonObject>>() {
      public void handle(Message<JsonObject> reply) {
        System.out.println(reply.body().toString());
        assertEquals("true", "true");
        testComplete();
      }
    });


    //First delete everything
    JsonObject json = new JsonObject().putString("collection", "testcoll")
        .putString("action", "delete").putObject("matcher", new JsonObject());

    eb.send("test.persistor", json, new Handler<Message<JsonObject>>() {
      public void handle(Message<JsonObject> reply) {
        assertEquals("ok", reply.body().getString("status"));
        final int numDocs = 1;
        final AtomicInteger count = new AtomicInteger(0);
        for (int i = 0; i < numDocs; i++) {
          JsonObject doc = new JsonObject().putString("name", "joe bloggs").putNumber("age", 40).putString("cat-name", "watt");
          JsonObject json = new JsonObject().putString("collection", "testcoll").putString("action", "save").putObject("document", doc);
          eb.send("test.persistor", json, new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> reply) {
              assertEquals("ok", reply.body().getString("status"));
              if (count.incrementAndGet() == numDocs) {
                JsonObject matcher = new JsonObject().putString("name", "joe bloggs");

                JsonObject json = new JsonObject().putString("collection", "testcoll").putString("action", "find").putObject("matcher", matcher);

                eb.send("test.persistor", json, new Handler<Message<JsonObject>>() {
                  public void handle(Message<JsonObject> reply) {
                    assertEquals("ok", reply.body().getString("status"));
                    JsonArray results = reply.body().getArray("results");
                    assertEquals(numDocs, results.size());
                    testComplete();
                  }
                });
              }
            }
          });
        }


      }
    });
  }*/

  /*
  @Test
  public void testCommand() throws Exception {
    JsonObject ping = new JsonObject().putString("action", "command")
        .putString("command", "{ping:1}");

    eb.send("test.persistor", ping, new Handler<Message<JsonObject>>() {
      public void handle(Message<JsonObject> reply) {
        Number ok = reply.body()
            .getObject("result")
            .getNumber("ok");

        assertEquals(1.0, ok);
        testComplete();
      }
    });
  }*/

}
