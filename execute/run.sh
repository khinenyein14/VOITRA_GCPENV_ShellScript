#!/bin/bash
source ../../GCPTest/.env

help_opt="-h"
run_opt="-r"

if [ "$1" = "$help_opt" ]
then
  echo "The instructions to run the program are as follows:"
  echo "1. Upload the audio files under the folder named audio."
  echo "2. The allowed file extensions are .mp3 and .wav types."
  echo "3. When uploading is finished, type . ./run.sh -r to run the program."
  echo "4. Speech To Text results will be saved under the folder named result."
elif [ "$1" = "$run_opt" ]
then
  python3 ../../GCPTest/bin/GCPTest.py
else
  echo "Please type the correct options."
fi