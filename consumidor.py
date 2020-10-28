import pika, sys, os
import json
import pymongo
from pymongo import MongoClient


def main():
  parameters = pika.URLParameters('amqps://mwtqvscf:K8LVCjOMl4ofn6LU9ZlXcYfH96tedpX1@woodpecker.rmq.cloudamqp.com/mwtqvscf')
  connection = pika.BlockingConnection(parameters)
  channel = connection.channel()

  channel.queue_declare(queue='hello')
  client = MongoClient('mongodb+srv://marcos:Mongo#4321@arquitetura.aafrk.mongodb.net/arquitetura?retryWrites=true&w=majority')
  db = client.get_database('arquitetura')
  records = db.teste
  #new_student = {"name":"joao", "xpto":"abcd"}
  #records.count_documents({})
  #print(new_student)
  #records.insert_one(new_student)
  
  def callback(ch, method, properties, body):
      r  = json.loads(body)
      records.insert_one(r)
      #print(r)
      #print(" [x] Received %r" % body)

  channel.basic_consume(queue='hello', on_message_callback=callback, auto_ack=True)

  print(' [*] Waiting for messages. To exit press CTRL+C')
  channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)