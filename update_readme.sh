#!/bin/bash

set -e

trap "rm -f README.md.new" EXIT

awk '
/Command-line Options/ { seen = 1; }
{ if (!seen) print $0; }
' README.md > README.md.new

echo "Command-line Options" >> README.md.new
echo "====================" >> README.md.new
echo >> README.md.new

./mutilate --help | sed "s/^/    /" >> README.md.new

mv -f README.md.new README.md

trap "" EXIT
