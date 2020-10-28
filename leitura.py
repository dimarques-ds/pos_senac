import pika, requests, json

url = "https://senac-api.herokuapp.com/"

response = requests.get(url)
data = response.text
#print(data)

parsed = json.loads(data)
#print(parsed)
#print(parsed["cadastros"][0])

parameters = pika.URLParameters('amqps://mwtqvscf:K8LVCjOMl4ofn6LU9ZlXcYfH96tedpX1@woodpecker.rmq.cloudamqp.com/mwtqvscf')

connection = pika.BlockingConnection(parameters)

channel = connection.channel()

channel.queue_declare(queue='hello')

for i in parsed["cadastros"]:
  del i["veiculo"]["placa"]
  channel.basic_publish(exchange='', routing_key='hello', body= json.dumps(i)) 
  print(i)
  print("\n CORTE")

#channel.basic_publish(exchange='', routing_key='hello', body= json.dumps(parsed))

print(" [x] Sent ")
connection.close()