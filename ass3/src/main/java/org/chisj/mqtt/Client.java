package org.chisj.mqtt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class Client {
    public static ArrayList<Integer> messages_f0 = new ArrayList<Integer>();
    public static ArrayList<Integer> messages_f1 = new ArrayList<Integer>();
    public static ArrayList<Integer> messages_f2 = new ArrayList<Integer>();
    public static ArrayList<Double> times_fast_q0 = new ArrayList<Double>();
    public static ArrayList<Double> times_fast_q1 = new ArrayList<Double>();
    public static ArrayList<Double> times_fast_q2 = new ArrayList<Double>();

    public static final String HOST = "tcp://comp3310.ddns.net";
    public static final String TOPIC1 = "counter/fast/q0";
    public static final String TOPIC2 = "counter/fast/q1";
    public static final String TOPIC3 = "counter/fast/q2";
    public static final String TOPIC4 = "counter/slow/q0";
    public static final String TOPIC5 = "counter/slow/q1";
    public static final String TOPIC6 = "counter/slow/q2";
    public static final String TOPIC_sent ="$SYS/broker/load/messages/sent/1min";
    public static final String TOPIC_heap ="$SYS/broker/heap/current";
    public static final String TOPIC_active ="$SYS/broker/clients/active";
    public static final String TOPICtest = "zzz";
    private static final String clientid = "3310-u5686922";
    private static final int time = 300;
    public static int error;
    public static int loss;
    public static int out_of_order_f0;
    public static int out_of_order_f1;
    public static int out_of_order_f2;
    private static MqttClient client;
    private MqttConnectOptions options;
    private String userName = "students";
    private String passWord = "33106331";
    private MqttMessage message;
    private ScheduledExecutorService scheduler;
    private MqttTopic topic;

    private void start(String[] topics, int[] qos) {
        try {
            // host为主机名，clientid即连接MQTT的客户端ID，一般以唯一标识符表示，MemoryPersistence设置clientid的保存形式，默认为以内存保存
            client = new MqttClient(HOST, clientid, new MemoryPersistence());
            // MQTT的连接设置
            options = new MqttConnectOptions();
            // 设置是否清空session,这里如果设置为false表示服务器会保留客户端的连接记录，这里设置为true表示每次连接到服务器都以新的身份连接
            options.setCleanSession(true);
            // 设置连接的用户名
            options.setUserName(userName);
            // 设置连接的密码
            options.setPassword(passWord.toCharArray());
            // 设置超时时间 单位为秒
            options.setConnectionTimeout(10);
            // 设置会话心跳时间 单位为秒 服务器会每隔1.5*20秒的时间向客户端发送个消息判断客户端是否在线，但这个方法并没有重连的机制
            options.setKeepAliveInterval(20);
//             设置回调
            client.setCallback(new org.chisj.mqtt.PushCallback());
            topic = client.getTopic(TOPICtest);

            //setWill方法，如果项目中需要知道客户端是否掉线可以调用该方法。设置最终端口的通知消息
            //options.setWill(topic, "close".getBytes(), 2, true);

            client.connect(options);
            //订阅消息

            client.subscribe(topics,qos);



            Thread.sleep(time*1000);


//            client.disconnect();
//            client.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws MqttException {

        Client client = new Client();
        int[] Qos  = {0,1,2};
        String[] topics1 = {TOPIC1,TOPIC2,TOPIC3};
        String[] topics2 = {TOPIC4,TOPIC5,TOPIC6};
        client.start(topics1,Qos);



        Collections.sort(messages_f0);
        Collections.sort(messages_f1);
        Collections.sort(messages_f2);
        Set<Integer> set_f0 = new HashSet<Integer>(messages_f0);
        Set<Integer> set_f1 = new HashSet<Integer>(messages_f1);
        Set<Integer> set_f2 = new HashSet<Integer>(messages_f2);
        System.out.println("The rate of messages in fast q0 is: " +(double)messages_f0.size()/time);
        System.out.println("The rate of messages in fast q1 is: " +(double)messages_f1.size()/time);
        System.out.println("The rate of messages in fast q2 is: " +(double)messages_f2.size()/time);
        System.out.println("The rate of message loss in fast q0 is: "+ (double)((messages_f0.get(messages_f0.size()-1)-messages_f0.get(0))- messages_f0.size()+1 )/(double)(messages_f0.get(messages_f0.size()-1)-messages_f0.get(0)));
        System.out.println("The rate of message loss in fast q1 is: "+ (double)((messages_f1.get(messages_f1.size()-1)-messages_f1.get(0))- messages_f1.size()+1 )/(double)(messages_f1.get(messages_f1.size()-1)-messages_f1.get(0)));
        System.out.println("The rate of message loss in fast q2 is: "+ (double)((messages_f2.get(messages_f2.size()-1)-messages_f2.get(0))- messages_f2.size() +1 )/(double)(messages_f2.get(messages_f2.size()-1)-messages_f2.get(0)));

        System.out.println("The rate of duplicated messages in fast q0 is: "+(double)(messages_f0.size()-set_f0.size())/messages_f0.size());
        System.out.println("The rate of duplicated messages in fast q1 is: "+(double)(messages_f1.size()-set_f1.size())/messages_f1.size());
        System.out.println("The rate of duplicated messages in fast q2 is: "+(double)(messages_f2.size()-set_f2.size())/messages_f2.size());
        System.out.println("The rate of out-of-order messages in fast q0 is: "+(double)out_of_order_f0/messages_f0.size());
        System.out.println("The rate of out-of-order messages in fast q1 is: "+(double)out_of_order_f1/messages_f1.size());
        System.out.println("The rate of out-of-order messages in fast q2 is: "+(double)out_of_order_f2/messages_f2.size());

        System.out.println("the mean inter-message-gap in fast q0 is: "+sum(times_fast_q0)/times_fast_q0.size());
        System.out.println("the mean inter-message-gap in fast q1 is: "+sum(times_fast_q1)/times_fast_q1.size());
        System.out.println("the mean inter-message-gap in fast q2 is: "+sum(times_fast_q2)/times_fast_q2.size());
        System.out.println("the gap-variation in fast q0 is: "+standard_deviation(sum(times_fast_q0)/times_fast_q0.size(),times_fast_q0));
        System.out.println("the gap-variation in fast q1 is: "+standard_deviation(sum(times_fast_q1)/times_fast_q1.size(),times_fast_q1));
        System.out.println("the gap-variation in fast q2 is: "+standard_deviation(sum(times_fast_q2)/times_fast_q2.size(),times_fast_q2));
//        System.out.println(messages_f2);
        System.out.println(messages_f2.size());
        System.out.println((messages_f2.get(messages_f2.size()-1)-messages_f2.get(0)));
//        System.out.println(messages_f0);
//        System.out.println(messages_f0.size());
//        System.out.println((messages_f0.get(messages_f0.size()-1))-messages_f0.get(0));
        pub(client);
        Client.client.disconnect();
        Client.client.close();


    }
    private static void pub(Client client) throws MqttException {
        client.message = new MqttMessage();
        client.message.setQos(0);
        client.message.setRetained(true);
        client.message.setPayload("?".getBytes());
        client.topic.publish(client.message);
    }

    private static double standard_deviation(double mean, ArrayList<Double> table) {
        double temp = 0;

        for (int i = 0; i < table.size(); i++)
        { double val = table.get(i);
            double squrDiffToMean = Math.pow(val - mean, 2);
            temp += squrDiffToMean;
        }
        double meanOfDiffs = temp / (table.size());
        return Math.sqrt(meanOfDiffs);
    }
    private static double sum(ArrayList<Double> m){
        double sum = 0;
        for(Double d : m)
            sum += d;
        return sum;
    }

}