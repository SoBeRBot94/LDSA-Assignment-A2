#!/bin/bash

lineCount_en=`/usr/bin/wc --lines ./europarl-v7.bg-en.en`
lineCount_bg=`/usr/bin/wc --lines ./europarl-v7.bg-en.bg`

# Print Only The Number of Lines

output_en=`echo $lineCount_en | /usr/bin/awk '{print $1}'`
output_bg=`echo $lineCount_bg | /usr/bin/awk '{print $1}'`

echo "Line Count of English Dataset: $output_en"
echo
echo "Line Count of Bulgarian Dataset: $output_bg"
