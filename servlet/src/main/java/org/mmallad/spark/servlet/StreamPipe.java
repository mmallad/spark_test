package org.mmallad.spark.servlet;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import javax.websocket.*;
import javax.websocket.server.ServerEndpoint;

/**
 * Created by Dipak Malla
 * Date: 9/7/14
 */
@ServerEndpoint("/stream")
public class StreamPipe {
    private final MqttClientPersistence mqttClientPersistence = new MemoryPersistence();
    private MqttClient mqttClient;
    public String serverUrl = "tcp://localhost:1883";
    @OnOpen
    public void onOpen(Session session, EndpointConfig conf){
        try {
            mqttClient = new MqttClient(serverUrl, MqttClient.generateClientId(), mqttClientPersistence);
            mqttClient.connect();
            mqttClient.setCallback(new MqttHooker(session));
            mqttClient.subscribe("stream/process", 0);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
    @OnClose
    public void close(Session session, CloseReason reasonse){
        try {
            mqttClient.disconnect();
            mqttClient.close();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
    @OnError
    public void error(Session session, Throwable t){

    }
    @OnMessage
    public void OnMessage(Session session, String msg) {

    }
}
