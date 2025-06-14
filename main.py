from confluent_kafka import Consumer, Producer
import logging
import json
from pymongo import MongoClient
from contextlib import closing
import threading

from config import source_config, target_config, source_topic, target_topic, mongo_url, mongo_db, mongo_collection


# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler("kafka_bridge.log", encoding='utf-8'), 
        logging.StreamHandler() 
    ]
)
logger = logging.getLogger(__name__)


# Check connection to  Kafka source and target
def check_kafka_connection(conf, name="Kafka"):
    """
    Kiểm tra kết nối tới Kafka bằng cách tạo Producer và gửi test message.
    """
    try:
        logging.info(f"Kiểm tra kết nối tới {name}...")
        producer = Producer(conf)
        # Gửi message test, topic có thể phải tồn tại sẵn
        producer.produce(target_topic, value=b'connection_test')
        producer.flush()
        logging.info(f"Kết nối tới {name} thành công.")
        return True
    except Exception as e:
        logging.error(f"Lỗi kết nối tới {name}: {e}")
        return False

#Create consumer
def create_consumer(conf, topic_name : str):
    """
    Tạo Kafka Consumer và đăng ký topic.
    """
    try:
        consumer = Consumer(conf)
        consumer.subscribe([topic_name])
        logging.info(f"Consumer subcribe successfully!")
        return consumer
    except Exception as e:
        logging.error(f"Lỗi tạo consumer: {e}")
        return None

# Create producer
def create_producer(conf : dict):
    """
    Tạo Kafka Producer.
    """
    try:
        producer = Producer(conf)
        logging.info("Producer created successfully!")
        return producer
    except Exception as e:
        logging.error(f"Lỗi tạo producer: {e}")
        return None

# Forward messages from source to target
def forward_messages(consumer: Consumer, producer: Producer):
    """
    Đọc message từ consumer và produce lại sang producer.
    
    Args:
        consumer (Consumer): Kafka consumer instance.
        producer (Producer): Kafka producer instance.
    """
    logging.info("Bắt đầu đọc và forward dữ liệu...")
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                logging.debug("Không nhận được message, tiếp tục...")
                continue
            if msg.error():
                logging.error(f"Lỗi consumer: {msg.error()}")
                continue
            
            raw_value = msg.value()
            logging.debug(f"Nhận message, kiểu: {type(raw_value)}, nội dung: {raw_value}")

            # Xử lý các kiểu dữ liệu của raw_value
            if isinstance(raw_value, dict):
                try:
                    # Chuyển dictionary sang JSON string và encode sang bytes
                    raw_value = json.dumps(raw_value, ensure_ascii=False, default=str).encode('utf-8')
                    logging.debug(f"Đã chuyển dict sang JSON bytes: {raw_value}")
                except Exception as e:
                    logging.error(f"Lỗi khi mã hóa dict sang JSON: {e}, nội dung: {raw_value}")
                    continue
            elif isinstance(raw_value, str):
                # Nếu là string, encode sang bytes
                raw_value = raw_value.encode('utf-8')
                logging.debug(f"Đã encode string sang bytes: {raw_value}")
            elif isinstance(raw_value, bytes):
                # Nếu đã là bytes, giữ nguyên
                logging.debug(f"Sử dụng bytes gốc: {raw_value}")
            elif raw_value is None:
                logging.warning("Nhận giá trị None, bỏ qua...")
                continue
            else:
                logging.error(f"Kiểu dữ liệu không hỗ trợ: {type(raw_value)}, nội dung: {raw_value}")
                continue

            # Gửi message tới target topic
            try:
                producer.produce(topic=target_topic, value=raw_value)
                logging.info(f"Đã gửi message tới topic {target_topic}: {msg.value()}")
            except Exception as e:
                logging.error(f"Lỗi khi produce message: {e}, nội dung: {raw_value}")
                continue

    except KeyboardInterrupt:
        logging.info("Dừng chương trình theo yêu cầu người dùng.")
    except Exception as e:
        logging.error(f"Lỗi trong quá trình forward message: {e}")
    finally:
        producer.flush()  # Đảm bảo tất cả message được gửi
        consumer.close()
        logging.info("Consumer và producer đã đóng kết nối.")

# Preview messages from source for debugging
def preview_messages_from_source(consumer, num_messages = 5):
    """
    In ra một vài message từ Kafka source để kiểm tra dữ liệu.
    """
    logging.info(f"Đang lấy {num_messages} message từ Kafka nguồn để kiểm tra...")
    count = 0
    try:
        while count < num_messages:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                logging.info("Không có message mới.")
                continue
            if msg.error():
                logging.error(f"Lỗi khi đọc message: {msg.error()}")
                continue

            logging.info(f"[{count+1}] Message từ nguồn: {msg.value().decode('utf-8')}")
            count += 1

    except Exception as e:
        logging.error(f"Lỗi khi đọc dữ liệu từ Kafka nguồn: {e}")

# Export messages from Kafka to MongoDB
def export_to_mongo(collection, consumer, max_empty_polls = 5):
    """
    Xuất dữ liệu từ Kafka consumer sang MongoDB collection.
    Args:
        collection (pymongo.collection.Collection): MongoDB collection để lưu dữ liệu.
        consumer (Consumer): Kafka consumer instance.
        max_empty_polls (int): Số lần không nhận được message trước khi dừng.
    """

    if collection is None:
        logging.error("Không thể kết nối tới MongoDB collection.")
        return
    logging.info("Kết nối tới MongoDB thành công.")

    while True:
        try:
            msg = consumer.poll(timeout=1.0)

            if msg.error():
                logging.error(f"Lỗi message: {msg.error()}")
                continue

            try:
                value = msg.value().decode('utf-8')

                try:
                    doc = json.loads(value)
                except json.JSONDecodeError:
                    doc = {"message": value}

                result = collection.insert_one(doc)
                logging.info(f"Đã lưu message với _id: {result.inserted_id}")

            except Exception as e:
                logging.error(f"Lỗi khi xử lý message: {e}, nội dung: {msg.value()}")

        except KeyboardInterrupt:
            logging.info("Dừng chương trình theo yêu cầu người dùng.")
            break

    consumer.close()
    logging.info("Đã đóng Kafka consumer.")



def main():
    if not check_kafka_connection(source_config, "Kafka nguồn"):
        logging.error("Không thể kết nối tới Kafka nguồn, dừng chương trình.")
        return

    if not check_kafka_connection(target_config, "Kafka đích"):
        logging.error("Không thể kết nối tới Kafka đích, dừng chương trình.")
        return

    producer = create_consumer(target_config)
    source_consumer = create_consumer(source_config, source_topic)

    if producer is None or source_consumer is None:
        logging.error("Không thể tạo producer hoặc consumer cho forwarding.")
        return

    # Tạo consumer riêng cho export_to_mongo
    mongo_consumer = create_consumer(target_config, target_topic)
    if mongo_consumer is None:
        logging.error("Không thể tạo consumer cho MongoDB export.")
        return

    try:
        mongo_client = MongoClient(mongo_url)
        db = mongo_client[mongo_db]
        collection = db[mongo_collection]
    except Exception as e:
        logging.error(f"Lỗi khi kết nối MongoDB: {e}")
        return

    # Thread 1: Forward messages từ source đến target
    forward_thread = threading.Thread(
        target=forward_messages,
        args=(source_consumer, producer),
        name="ForwardThread"
    )

    # Thread 2: Export từ target topic sang MongoDB
    export_thread = threading.Thread(
        target=export_to_mongo,
        args=(collection, mongo_consumer),
        name="ExportMongoThread"
    )

    logging.info("Bắt đầu chạy song song forwarding và export to MongoDB.")
    forward_thread.start()
    export_thread.start()

    try:
        forward_thread.join()
        export_thread.join()
    except KeyboardInterrupt:
        logging.info("Người dùng yêu cầu dừng chương trình.")
    finally:
        source_consumer.close()
        mongo_consumer.close()
        mongo_client.close()
        logging.info("Đã đóng kết nối Kafka và MongoDB.")

if __name__ == "__main__":
    main()




























