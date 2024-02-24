# Bitebot #

## About ##

Dining Concierge Chatbot built as part of the course CS-GY 9223 Cloud Computing at New York University.

## Usage ##

1. Clone the repository.
2. Replace `/assets/js/sdk/apigClient.js` with your own SDK file from API
   Gateway.
3. Open `chat.html` in any browser.
4. Create the 3 lambda functions described here.
5. Create a Lex Chatbot with relevant intents and slots.
6. Setup DynamoDB and ElasticSearch to query and serve the recommendations.
7. Setup SQS for message passing between the services.
8. Start sending messages to test the chatbot interaction.