package org.vertx.mods.orientdb.test.integration.java;

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
  public void start() {
    eb = vertx.eventBus();
    /*JsonObject config = new JsonObject();
    config.putString("address", "test.persistor");
    config.putString("db_name", System.getProperty("vertx.mongo.database", "test_db"));
    config.putString("host", System.getProperty("vertx.mongo.host", "localhost"));
    config.putNumber("port", Integer.valueOf(System.getProperty("vertx.mongo.port", "27017")));
    String username = System.getProperty("vertx.mongo.username");
    String password = System.getProperty("vertx.mongo.password");
    if (username != null) {
      config.putString("username", username);
      config.putString("password", password);
    }
    config.putBoolean("fake", false);*/
    container.deployModule(System.getProperty("vertx.modulename"), null, 1, new AsyncResultHandler<String>() {
      public void handle(AsyncResult<String> ar) {
        if (ar.succeeded()) {
          PersistorTest.super.start();
        } else {
          ar.cause().printStackTrace();
        }
      }
    });
  }

  @Test
  public void testPersistor() throws Exception {

    JsonObject getEdgeMessage = new JsonObject().putString("action", "get_edge")
        .putString("class", "edgeClass").putObject("criteria", new JsonObject());

    eb.send("vertx.orientdbpersistor", getEdgeMessage, new Handler<Message<JsonObject>>() {
      public void handle(Message<JsonObject> reply) {
        assertEquals("true", "true");
      }
    });

    /*
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
    });*/
  }

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
