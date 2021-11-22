#!/bin/bash
ssh -i .ssh/mastracking.pem -p 3333 pi@18.191.90.55 python3 /home/pi/tic5_rbpi/read_single_temp.py $1