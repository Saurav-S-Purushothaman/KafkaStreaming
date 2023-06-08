#producer 

#imports
import time
import cv2
from kafka import KafkaClient
from kafka import KafkaProducer

# connect to kafka
kafka = KafkaClient(bootstrap_servers ="localhost:9092")
producer = KafkaProducer(kafka)

# assign a topic 
topic = "mytopic"

# video emitter function 
def video_emitter(video):

    #open the video 
    video = cv2.VideoCapture(video)
    print(f"Emitting...")

    #read the file 
    while (video.isOpened):

        #read the image in each frame
        success,image =video.read()
        if not success:
            break

        # convert the image png 
        ret, jpeg = cv2.imencode(".png",image)

        # convert the image to bytes and send to kafka
        producer.send_message(topic, jpeg.tobytes())

        # to reduce cpu usage create sleep time of 0.2 sec
        time.sleep(0.2)

    video.release()
    print("done emitting!!")

        
if __name__ == "__main__":
    video_emitter("sample_video.mp4")

