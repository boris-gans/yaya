#!/bin/bash

# start_services.sh

# Function to start a service in a new terminal with venv and PYTHONPATH
start_service() {
    local name=$1
    local command=$2
    
    # For macOS, using osascript to open new terminal windows
    # Change to microservices directory and set it as PYTHONPATH
    osascript -e "tell app \"Terminal\"
        do script \"echo -e '=== Starting $name ===\n' && cd $(pwd)/microservices && source ../.venv/bin/activate && export PYTHONPATH=$(pwd)/microservices && $command\"
    end tell"
}

# Always start main.py with correct uvicorn format
start_service "Main API" "uvicorn main:app --reload --port 8000"
echo -e "Started Main API\n"

# Process command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --db-reads)
            start_service "DB Reads" "cd db_reads && uvicorn db_reads:app --reload --port 8001"
            echo -e "Started DB Reads Service\n"
            shift
            ;;
        --db-writes)
            start_service "DB Writes" "python -m db_writes.db_writes"
            echo -e "Started DB Writes Service\n"
            shift
            ;;
        --background)
            start_service "Celery Worker" "celery -A background_writes.celery_worker worker --loglevel=INFO"
            start_service "Background Writes" "python -m background_writes.background_writes"
            echo -e "Started Background Processing Services\n"
            shift
            ;;
        --recommendation)
            start_service "Recommendation" "cd recommendation && uvicorn recommendation:app --reload --port 8002"
            echo -e "Started Recommendation Service\n"
            shift
            ;;
        *)
            echo "Unknown parameter: $1"
            shift
            ;;
    esac
done

echo "All specified services have been started!"
echo "Use Ctrl+C in individual terminals to stop services"