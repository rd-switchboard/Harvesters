#!/bin/bash 

xsltproc -v cern_marc21.xsl input_sample_full.xml 2>error.txt | xmllint --format --output output_sample_full.xml -
