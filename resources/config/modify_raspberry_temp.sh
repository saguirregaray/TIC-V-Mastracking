#!/bin/bash
ssh -i ./resources/config/mastracking.pem -p 3333 pi@3.145.202.20 python3 /home/pi/tic5_rbpi/set_temp.py $1 $2