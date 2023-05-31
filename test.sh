#!/bin/bash

set -e

cd "$( dirname "${BASH_SOURCE[0]}" )"

coverage run -m pytest && coverage report
