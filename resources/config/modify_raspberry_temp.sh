#!/bin/bash
ssh -i ./resources/config/mastracking.pem -p 3333 pi@18.191.90.55 python3 /home/pi/tic5_rbpi/set_temp.py $1 $2