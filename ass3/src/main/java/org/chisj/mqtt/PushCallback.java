package org.chisj.mqtt;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;



public class PushCallback implements MqttCallback {
    private static double time_fast_q0;
    private static double time_fast_q1;
    private static double time_fast_q2;
    private static double start_fast_q0;
    private static double start_fast_q1;
    private static double start_fast_q2;
    private static int last_fast_q0;
    private static int last_fast_q1;
    private static int last_fast_q2;
    public void connectionLost(Throwable cause) {
        // 连接丢失后，一般在这里面进行重连
        System.out.println("连接断开，可以做重连");
    }

    public void deliveryComplete(IMqttDeliveryToken token) {
//        System.out.println("deliveryComplete " + token.isComplete());
    }

    public void messageArrived(String topic, MqttMessage message) throws Exception {

        String mes = new String(message.getPayload());

        control(mes,topic);


        // subscribe后得到的消息会执行到这里面



//        System.out.println("接收消息主题 : " + topic);
//        System.out.println("接收消息Qos : " + message.getQos());
//        System.out.println("接收消息内容 : " + mes);
//        System.out.println();

    }



    private void control(String mes, String topic){
        if (topic.contains( "q0")){

            try{Client.messages_f0.add(Integer.parseInt(mes));
                if (Integer.parseInt(mes)-last_fast_q0==1){
                    start_fast_q0 = System.currentTimeMillis();
                    Client.times_fast_q0.add(start_fast_q0-time_fast_q0);
                }
                if (Integer.parseInt(mes)-last_fast_q0<1){
                    Client.out_of_order_f0+=1;
                }
                last_fast_q0 = Integer.parseInt(mes);
            } catch (NumberFormatException e) {
                Client.error+=1;
            }
            time_fast_q0=System.currentTimeMillis();
        }
        if (topic.contains( "q1")){

            try{Client.messages_f1.add(Integer.parseInt(mes));

                if (Integer.parseInt(mes)-last_fast_q1==1){
                    start_fast_q1 = System.currentTimeMillis();
                    Client.times_fast_q1.add(start_fast_q1-time_fast_q1);
                }
                if (Integer.parseInt(mes)-last_fast_q1<1){
                    Client.out_of_order_f1+=1;
                }
                last_fast_q1 = Integer.parseInt(mes);
            } catch (NumberFormatException e) {
                Client.error+=1;
            }
            time_fast_q1=System.currentTimeMillis();
        }
        if (topic.contains( "q2")){
                        try{Client.messages_f2.add(Integer.parseInt(mes));
                if (Integer.parseInt(mes)-last_fast_q2==1){
                    start_fast_q2 = System.currentTimeMillis();
                    Client.times_fast_q2.add(start_fast_q2-time_fast_q2);
                }
                if (Integer.parseInt(mes)-last_fast_q2<1){
                    Client.out_of_order_f2+=1;
                }
                last_fast_q2= Integer.parseInt(mes);
            } catch (NumberFormatException e) {
                Client.error+=1;
            }
           time_fast_q2=System.currentTimeMillis();

        }
        if(topic.contains( "sent")){
            Client.sent.add(mes);
        }
        if(topic.contains( "heap")){
            Client.heap.add(mes);
        }
        if(topic.contains( "active")){
            Client.active.add(mes);
        }
    }
}