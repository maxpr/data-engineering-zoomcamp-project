#!/bin/bash

cd /app/terraform
terraform init
terraform destroy -auto-approve
