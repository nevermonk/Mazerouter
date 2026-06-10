#!/bin/bash

go build -buildvcs=false -o bin/megarouter ./
chmod u+x bin/megarouter