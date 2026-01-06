#!/bin/bash

# Simple wrapper script to start the entire distributed event streaming system
# Usage: ./start.sh [options]

cd "$(dirname "$0")"
node scripts/startAll.js "$@"
