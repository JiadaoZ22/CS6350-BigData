How to run this program in AWS:

Uploaded the source data file (review.json) and jar file into one folder in AWS first.
You can use the following command in emr steps:
spark-submit --deploy-mode cluster --class "YelpAnalyse" [path of jar file] [path of source dataset] [stars/polarity] [path of output directory] [number of folds] [number of holdout]
spark-submit --deploy-mode cluster --class "YelpAnalyse" 
s3://6350final/6350final_2.11-0.1.jar
s3://6350final/review.json
polarity
s3://6350final/output
5
3