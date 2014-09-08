package org.mmallad.spark.servlet;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import javax.websocket.Session;

/**
 * Created by Dipak Malla
 * Date: 9/7/14
 */
public class MqttHooker implements MqttCallback {
    public Session sessionList;
    public MqttHooker(Session session){
        sessionList = session;
    }
    @Override
    public void connectionLost(Throwable cause) {
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        String data = new String(message.getPayload());
        sessionList.getBasicRemote().sendText(data);
        System.out.println(data);
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }
}
