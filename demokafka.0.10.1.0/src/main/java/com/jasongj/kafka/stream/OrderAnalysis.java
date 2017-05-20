package com.jasongj.kafka.stream;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import com.jasongj.kafka.stream.model.Item;
import com.jasongj.kafka.stream.model.Order;
import com.jasongj.kafka.stream.model.User;
import com.jasongj.kafka.stream.serdes.SerdesFactory;
import com.jasongj.kafka.stream.timeextractor.OrderTimestampExtractor;


/**
 * 
 * @author wuchangzheng
 * 
 * 
kafka大作业
试题定义
每5秒输出过去1小时18岁到35岁用户所购买的商品中，每种品类销售额排名前十的订单汇总信息。
试题要求
  ● 使用数据内的时间(Event Time)作为timestamp
  ● 每5秒输出一次
  ● 每次计算到输出为止过去1小时的数据
  ● 支持订单详情和用户详情的更新和增加
  ● 输出字段包含时间窗口（起始时间，结束时间），品类（category），商品名（item_name），销量（quantity），单价（price），总销售额，该商品在该品类内的销售额排名
  ● 将代码及创建相关Topic的脚本上传至Github
  ● 代码须包含读文件中的数据并发布到Kafka的Producer；自定义的Partitioner；Kafka Stream应用的代码（DSL）
试题提示
订单Topic作为KStream，用户和商品Topic作为KTable 
 * 
 * 
 *
 */
public class OrderAnalysis {

	public static void main(String[] args) throws IOException, InterruptedException {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-order-analysis");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka0:19092");
		props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "zookeeper0:12181/kafka");
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, OrderTimestampExtractor.class);

		KStreamBuilder streamBuilder = new KStreamBuilder();
		KStream<String, Order> orderStream = streamBuilder.stream(Serdes.String(), SerdesFactory.serdFrom(Order.class), "orders");//将order作为kstream
		KTable<String, User> userTable = streamBuilder.table(Serdes.String(), SerdesFactory.serdFrom(User.class), "users", "users-state-store");//将users作为ktable
		KTable<String, Item> itemTable = streamBuilder.table(Serdes.String(), SerdesFactory.serdFrom(Item.class), "items", "items-state-store");//将items作为ktable
//		itemTable.toStream().foreach((String itemName, Item item) -> System.out.printf("Item info %s-%s-%s-%s\n", item.getItemName(), item.getAddress(), item.getType(), item.getPrice()));
		KStream<String, String> kStream = 
				orderStream
				.leftJoin(userTable, (Order order, User user) -> OrderUser.fromOrderUser(order, user), Serdes.String(), SerdesFactory.serdFrom(Order.class)) //order left join user
				.filter((String userName, OrderUser orderUser) -> orderUser.age>=18 && orderUser.age<=35) //过滤18到35岁的用户
				.map((String userName, OrderUser orderUser) -> new KeyValue<String, OrderUser>(orderUser.itemName, orderUser))//将key调整为itemname
				.through(Serdes.String(), SerdesFactory.serdFrom(OrderUser.class), (String key, OrderUser orderUser, int numPartitions) -> (orderUser.getItemName().hashCode() & 0x7FFFFFFF) % numPartitions, "orderuser-repartition-by-item")
				.leftJoin(itemTable, (OrderUser orderUser, Item item) -> OrderUserItem.fromOrderUser(orderUser, item), Serdes.String(), SerdesFactory.serdFrom(OrderUser.class))
				
				.map((itemName,orderUserItem)->KeyValue.<String,OrderUserItem>pair("品名："+orderUserItem.itemName+" ,品类 :" + orderUserItem.itemType + " ,单价" +orderUserItem.itemPrice, orderUserItem))
				.groupByKey(Serdes.String(), SerdesFactory.serdFrom(OrderUserItem.class)).aggregate(
						() -> OrderUserItemStatic.from(0, 0.00),
						(aggKey, value, aggregate) -> OrderUserItemStatic.from(aggregate.quantity+value.quantity,
								aggregate.amount+value.quantity*value.itemPrice), 
						TimeWindows.of(1000*3600).advanceBy(1000),
						SerdesFactory.serdFrom(OrderUserItemStatic.class), 
						"Totoal").toStream()
				
				.map((Windowed<String> window, OrderUserItemStatic value) -> {
					return new KeyValue<String, String>(window.key(), String.format("%s, 销售数量 %s ,销售额=%s, start=%s, end=%s\n",window.key(), value.quantity,value.amount, new Date(window.window().start()), new Date(window.window().end())));
				});
		
		//kStream.
			
				kStream.foreach((k,v)->System.out.println(v));
				
				
				KafkaStreams kafkaStreams = new KafkaStreams(streamBuilder, props);
				kafkaStreams.cleanUp();
				kafkaStreams.start();
				System.out.println("计算商品销售开始，按任意健退出");
				System.in.read();
				kafkaStreams.close();
				kafkaStreams.cleanUp();
				
	}
	
	public static class OrderUser {
		private String userName;
		private String itemName;
		private long transactionDate;
		private int quantity;
		private String userAddress;
		private String gender;
		private int age;
		
		public String getUserName() {
			return userName;
		}

		public void setUserName(String userName) {
			this.userName = userName;
		}

		public String getItemName() {
			return itemName;
		}

		public void setItemName(String itemName) {
			this.itemName = itemName;
		}

		public long getTransactionDate() {
			return transactionDate;
		}

		public void setTransactionDate(long transactionDate) {
			this.transactionDate = transactionDate;
		}

		public int getQuantity() {
			return quantity;
		}

		public void setQuantity(int quantity) {
			this.quantity = quantity;
		}

		public String getUserAddress() {
			return userAddress;
		}

		public void setUserAddress(String userAddress) {
			this.userAddress = userAddress;
		}

		public String getGender() {
			return gender;
		}

		public void setGender(String gender) {
			this.gender = gender;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

		public static OrderUser fromOrder(Order order) {
			OrderUser orderUser = new OrderUser();
			if(order == null) {
				return orderUser;
			}
			orderUser.userName = order.getUserName();
			orderUser.itemName = order.getItemName();
			orderUser.transactionDate = order.getTransactionDate();
			orderUser.quantity = order.getQuantity();
			return orderUser;
		}
		
		public static OrderUser fromOrderUser(Order order, User user) {
			OrderUser orderUser = fromOrder(order);
			if(user == null) {
				return orderUser;
			}
			orderUser.gender = user.getGender();
			orderUser.age = user.getAge();
			orderUser.userAddress = user.getAddress();
			return orderUser;
		}
	}
	
	public static class OrderUserItem {
		private String userName;
		private String itemName;
		private long transactionDate;
		private int quantity;
		private String userAddress;
		private String gender;
		private int age;
		private String itemAddress;
		private String itemType;
		private double itemPrice;
		
		public String getUserName() {
			return userName;
		}

		public void setUserName(String userName) {
			this.userName = userName;
		}

		public String getItemName() {
			return itemName;
		}

		public void setItemName(String itemName) {
			this.itemName = itemName;
		}

		public long getTransactionDate() {
			return transactionDate;
		}

		public void setTransactionDate(long transactionDate) {
			this.transactionDate = transactionDate;
		}

		public int getQuantity() {
			return quantity;
		}

		public void setQuantity(int quantity) {
			this.quantity = quantity;
		}

		public String getUserAddress() {
			return userAddress;
		}

		public void setUserAddress(String userAddress) {
			this.userAddress = userAddress;
		}

		public String getGender() {
			return gender;
		}

		public void setGender(String gender) {
			this.gender = gender;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

		public String getItemAddress() {
			return itemAddress;
		}

		public void setItemAddress(String itemAddress) {
			this.itemAddress = itemAddress;
		}

		public String getItemType() {
			return itemType;
		}

		public void setItemType(String itemType) {
			this.itemType = itemType;
		}

		public double getItemPrice() {
			return itemPrice;
		}

		public void setItemPrice(double itemPrice) {
			this.itemPrice = itemPrice;
		}

		public static OrderUserItem fromOrderUser(OrderUser orderUser) {
			OrderUserItem orderUserItem = new OrderUserItem();
			if(orderUser == null) {
				return orderUserItem;
			}
			orderUserItem.userName = orderUser.userName;
			orderUserItem.itemName = orderUser.itemName;
			orderUserItem.transactionDate = orderUser.transactionDate;
			orderUserItem.quantity = orderUser.quantity;
			orderUserItem.userAddress = orderUser.userAddress;
			orderUserItem.gender = orderUser.gender;
			orderUserItem.age = orderUser.age;
			return orderUserItem;
		}

		public static OrderUserItem fromOrderUser(OrderUser orderUser, Item item) {
			OrderUserItem orderUserItem = fromOrderUser(orderUser);
			if(item == null) {
				return orderUserItem;
			}
			orderUserItem.itemAddress = item.getAddress();
			orderUserItem.itemType = item.getType();
			orderUserItem.itemPrice = item.getPrice();
			return orderUserItem;
		}
	}
	
	public static class OrderUserItemStatic{
		public int getQuantity() {
			return quantity;
		}
		public void setQuantity(int quantity) {
			this.quantity = quantity;
		}
		public double getAmount() {
			return amount;
		}
		public void setAmount(double amount) {
			this.amount = amount;
		}
		int quantity;
		double amount;
		public static OrderUserItemStatic from(int quantity,double amount){
			OrderUserItemStatic obj = new OrderUserItemStatic();
			obj.quantity=quantity;
			obj.amount=amount;
			return obj;
		}
		
		
	}
}
