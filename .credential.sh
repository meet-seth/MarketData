#!/bin/bash

case $1 in 
    "get")
        echo "protocol=https"
        echo "host=github.com"
        echo "username=skyzack29"
        echo "password=$(/usr/bin/gh auth token --user skyzack29)"


esac