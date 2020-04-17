#!/bin/bash
aws lambda create-function \
    --region eu-west-1 \
    --function-name synergy-test-normaliser \
    --zip-file fileb://$(pwd)/../target/synergy-test-normaliser.jar \
    --role arn:aws:iam::979590819078:role/syn-evt-lambda \
    --handler synergy-test-normaliser.core.Route \
    --runtime java11 \
    --timeout 15 \
    --memory-size 1024 \
    --cli-connect-timeout 6000