#!/bin/bash
docker system prune -af
find . -name "__pycache__" -type d -exec rm -rf {} +
find . -name "*.pyc" -delete
echo "Cleanup complete!"
