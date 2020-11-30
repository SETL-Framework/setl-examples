#!/usr/bin/env bash

export AWS_ACCESS_KEY_ID=dummyaccess
export AWS_SECRET_ACCESS_KEY=dummysecret
export AWS_DEFAULT_REGION=us-east-1

aws --endpoint-url=http://dynamodb:8000 dynamodb delete-table --table-name orders_table
aws --endpoint-url=http://dynamodb:8000 dynamodb create-table --table-name orders_table --attribute-definitions AttributeName=orderId,AttributeType=S AttributeName=orderName,AttributeType=S --key-schema AttributeName=orderId,KeyType=HASH AttributeName=orderName,KeyType=RANGE --billing-mode=PAY_PER_REQUEST
aws --endpoint-url=http://dynamodb:8000 dynamodb put-item --table-name orders_table --item "{\"orderId\": {\"S\": \"1\"}, \"orderName\": {\"S\": \"firstOrder\"}}"
aws --endpoint-url=http://dynamodb:8000 dynamodb put-item --table-name orders_table --item "{\"orderId\": {\"S\": \"2\"}, \"orderName\": {\"S\": \"secondOrder\"}}"
aws --endpoint-url=http://dynamodb:8000 dynamodb put-item --table-name orders_table --item "{\"orderId\": {\"S\": \"3\"}, \"orderName\": {\"S\": \"thirdOrder\"}}"
aws --endpoint-url=http://dynamodb:8000 dynamodb put-item --table-name orders_table --item "{\"orderId\": {\"S\": \"4\"}, \"orderName\": {\"S\": \"fourthOrder\"}}"
aws --endpoint-url=http://dynamodb:8000 dynamodb put-item --table-name orders_table --item "{\"orderId\": {\"S\": \"5\"}, \"orderName\": {\"S\": \"fifthOrder\"}}"
