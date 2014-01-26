package org.vertx.mods;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

/**
 * Class Description
 * <p/>
 * User: jeremiahhall
 * Date: 1/26/14
 * Time: 12:28 PM
 */
public class OrientDBPersistor extends BusModBase implements Handler<Message<JsonObject>> {

    protected String address;
    protected String host;
    protected int port;
    protected String dbName;


    @Override
    public void handle(Message<JsonObject> jsonObjectMessage) {

    }
}
