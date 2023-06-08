#consumer

#imports 
from Flask import Flask, Response
from kafka import KafkaConsumer

# connect to kafka server and pass the topic we want to consume 
topic = "mytopic"
consumer = KafkaConsumer(topic, group_id = "view", bootstrap_server = ["0.0.0.0:9092"])

# continuously listent to connection and print messages as received 
app = Flask(__name__)

@app.route("/")
def index():
    # Return a multipart response
    return Response(
        kafkastream(),
        mimetype='multipart/x-mixed-replace; boundary=frame'
    )


def kafkastream():
    for msg in consumer:
        yield (b'--frame\r\n'
               b'Content-Type: image/png\r\n\r\n' + msg.value + b'\r\n\r\n')
        
if __name__ == "__main__":
    app.rund(host="0.0.0.0",debug = True)
