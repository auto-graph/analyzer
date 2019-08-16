# Analyzer
Analyze the data to determine graph schema

- Build a DAG with edges going from largest subset to superset


#Issues

 
1.  Deal with ID in actual data very common field
2.  Trim spaces in field Names ==> _FIXED_ 
3.  Drop empty objects ==> _FIXED_
4.  Drop edge creation for null sets as they are subset of all set (probably causing cycles)
5.  Clear the index before using it to eliminate any data from previous run ==> _FIXED_
