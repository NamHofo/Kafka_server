source_config = {
    'bootstrap.servers': '46.202.167.130:9094,46.202.167.130:9194,46.202.167.130:9294',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'kafka',
    'sasl.password': 'UnigapKafka@2024',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}

target_config = {
    'bootstrap.servers': 'localhost:9094',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'kafka',
    'sasl.password': 'UnigapKafka@2024',

}

source_topic = 'product_view'
target_topic = 'first_topic'