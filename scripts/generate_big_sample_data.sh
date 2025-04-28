#!/bin/bash
## Small script to generate a sample data of about 5.6GB

# Input file (original CSV file)
input_file="./testdata/sample_data.csv"

# Output file (the new file with 1000 appended copies)
output_file="./testdata/big_sample_data.csv"

# Ensure the output file is empty before starting
> "$output_file"

# Add the header from the input file to the output file
head -n 1 "$input_file" > "$output_file"

# Append the content of the input file (without the header) 100 times
for i in {1..10000}; do
    tail -n +2 "$input_file" >> "$output_file"
    echo "" >> "$output_file"
done

echo "Generated $output_file with 100 copies of $input_file content."
