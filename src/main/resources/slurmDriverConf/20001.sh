#!/bin/bash
#SBATCH --job-name=spacy_service
#SBATCH --time=00:10:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --output=spacy_service_%j.log
PORT=20001
INNER=9714
IMG="$HOME/spacy_test.sif"
INTOIMAGE="cd /usr/src/app"
UVI="uvicorn textimager_duui_spacy:app"
apptainer exec "$IMG" \
sh -c "$INTOIMAGE && $UVI --host 0.0.0.0 --port $PORT" &
PID=$!
trap 'kill $PID 2>/dev/null' EXIT
wait $PID
|