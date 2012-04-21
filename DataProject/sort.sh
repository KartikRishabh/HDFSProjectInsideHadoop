#!/bin/bash

if [ "$3" -eq  0 ]; then 
	sort -k$2,$2 -t',' $1 > $1_sorted
else 
	sort -k$2,$2 -n -t',' $1 > $1_sorted
fi
