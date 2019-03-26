#!/bin/sh

# TODO: fail if infrastructure is not running

docker-compose up --build producer_post producer_like producer_comment
