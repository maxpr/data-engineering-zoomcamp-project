#!/bin/bash

cd /app/terraform
terraform init
terraform apply -auto-approve
