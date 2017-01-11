package org.company.clientManager; 
import com.rabbitmq.client.*;
import org.apache.commons.lang.SerializationUtils;
 
import java.io.IOException;
 
public class RpcServer {
    public void sayHello(){
        System.out.println("hello");
    }
 
    public void sayGood(){
        System.out.println("Good");
    }
 
    public void run(String mName){
        Class <? extends RpcServer> c = this.getClass();
        try{
           c.getMethod(mName).invoke(this);
        }catch(Exception e){
           e.printStackTrace();
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException {
 
        String exchangeName = "rpc_exchange2";   //交换器名称
        String queueName = "rpc_queue";     //队列名称
        String routingKey = "mac key";  //路由键暂时用106的mac绑定
 
        ConnectionFactory factory = new ConnectionFactory();
        factory.setVirtualHost("/");
        factory.setHost("ip");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");
 
        try{
        System.out.println("RpcServer...");
        Connection connection = factory.newConnection();    //创建链接
 
        Channel channel = connection.createChannel();
        System.out.println("RpcServer2...");
 
        channel.exchangeDeclare(exchangeName, "direct");    //定义交换器
 
        channel.queueDeclare(queueName, false, false, false, null); //定义队列
 
        channel.queueBind(queueName, exchangeName, routingKey); //绑定队列
 
        QueueingConsumer consumer = new QueueingConsumer(channel);     //创建一个消费者
 
        channel.basicConsume(queueName,true,consumer);  //消费消息
 
        while (true){
 
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();  //获得一条消息
 
            String correlationID  = delivery.getProperties().getCorrelationId();    //获得额外携带的correlationID
 
            String replyTo = delivery.getProperties().getReplyTo(); //获得回调的队列路由键
 
            String body = (String) SerializationUtils.deserialize(delivery.getBody());  //获得请求的内容
 
            String responseMsg = "success"; //处理返回内容
            RpcServer server = new RpcServer();
            server.run(body);
 
            System.out.println(responseMsg);
            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .correlationId(correlationID)   //返回消息时携带 请求时传过来的correlationID
                    .build();
 
            channel.basicPublish("",replyTo,properties,SerializationUtils.serialize(responseMsg)); //返回处理结果
 
        }
        }catch(Exception e){
            e.printStackTrace();
        }
 
 
 
    }
 
 
 
}
