# Kafka Bridge and MongoDB Exporter

## 1. Overview

This project implements a bridge between Kafka and MongoDB. It reads messages from a source Kafka topic, forwards them to a target topic, and exports the messages from the target topic to a MongoDB collection.

---

## **📚 Table of Contents**

1. [Overview](#1-overview)  
2. [Features](#2-features)  
3. [Project Structure](#3-project-structure)  
4. [Installation](#4-installation)  
5. [Running the Script](#5-running-the-script)  
6. [Behavior](#6-behavior)  
7. [Future Improvements](#7-future-improvements)  
8. [Dependencies](#8-dependencies)  
9. [Contributing](#9-contributing)  
10. [License](#10-license)


---

## 2.  Features

- Connects to both source and target Kafka clusters.
- Reads messages from the source Kafka topic.
- Forwards messages from the source topic to a target topic.
- Exports messages from the target topic to MongoDB.
- Supports JSON and plain-text message formats.
- Auto-stops when no more new messages are available.

---

## 3. Project Structure

├── main.py         # Main script for running the pipeline

├── config.py               # Kafka and MongoDB configuration

├── kafka_bridge.log        # Log file for monitoring activity

├── requirements.txt        # List of required Python packages

└── README.md               # Project documentation

---

## 4. Installation

Install the required packages:

```bash
pip install -r requirements.txt
```

If `requirements.txt` is missing, create it with:

```bash
confluent-kafka
pymongo
```

---

## 5. Running the Script

To run the main pipeline:

```bash
python main.py
```

---

## 6. Behavior

- Messages from the **source topic** are forwarded to the **target topic**.
- Messages in the **target topic** are exported to MongoDB:
    - If the message is valid JSON, it's inserted as a document.
    - If not, it's stored as plain text with a `"message"` key.
- The script auto-terminates when no new messages are detected (end of topic).

---

## 7. Future Improvements

- Batch MongoDB inserts for better performance.
- Add data transformation or filtering before exporting.
- Containerization using Docker.
- Integration with logging and monitoring tools.

---

## 8. Dependencies

- `confluent-kafka`
- `pymongo`

Install them using:

```bash
pip install confluent-kafka pymongo
```

---

## 9. Contributing

Feel free to open issues or submit pull requests to improve the project.

---

## 10. License

MIT License
