import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from google.cloud import storage

spark = SparkSession.builder.appName('CS777 Project').getOrCreate()

maxlength = 1000
spark.conf.set("spark.sql.debug.maxToStringFields", maxlength)

bucket_name = "sheel-ravi-cs777-project-data"

storage_client = storage.Client()

blobs = storage_client.list_blobs(bucket_name)

list1 = []
for blob in blobs:
  path = f"gs://{bucket_name}/"
  path = path + str(blob.name)
  list1.append(str(path))
  print(path)


list1 = [file_path for file_path in list1 if file_path.endswith('.csv')]


from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, FloatType

schema = StructType([
    StructField("MI_dir_L5_weight", DoubleType(), True),
    StructField("MI_dir_L5_mean", DoubleType(), True),
    StructField("MI_dir_L5_variance", DoubleType(), True),
    StructField("MI_dir_L3_weight", DoubleType(), True),
    StructField("MI_dir_L3_mean", DoubleType(), True),
    StructField("MI_dir_L3_variance", DoubleType(), True),
    StructField("MI_dir_L1_weight", DoubleType(), True),
    StructField("MI_dir_L1_mean", DoubleType(), True),
    StructField("MI_dir_L1_variance", DoubleType(), True),
    StructField("MI_dir_L0_1_weight", DoubleType(), True),
    StructField("MI_dir_L0_1_mean", DoubleType(), True),
    StructField("MI_dir_L0_1_variance", DoubleType(), True),
    StructField("MI_dir_L0_01_weight", DoubleType(), True),
    StructField("MI_dir_L0_01_mean", DoubleType(), True),
    StructField("MI_dir_L0_01_variance", DoubleType(), True),
    StructField("H_L5_weight", DoubleType(), True),
    StructField("H_L5_mean", DoubleType(), True),
    StructField("H_L5_variance", DoubleType(), True),
    StructField("H_L3_weight", DoubleType(), True),
    StructField("H_L3_mean", DoubleType(), True),
    StructField("H_L3_variance", DoubleType(), True),
    StructField("H_L1_weight", DoubleType(), True),
    StructField("H_L1_mean", DoubleType(), True),
    StructField("H_L1_variance", DoubleType(), True),
    StructField("H_L0_1_weight", DoubleType(), True),
    StructField("H_L0_1_mean", DoubleType(), True),
    StructField("H_L0_1_variance", DoubleType(), True),
    StructField("H_L0_01_weight", DoubleType(), True),
    StructField("H_L0_01_mean", DoubleType(), True),
    StructField("H_L0_01_variance", DoubleType(), True),
    StructField("HH_L5_weight", DoubleType(), True),
    StructField("HH_L5_mean", DoubleType(), True),
    StructField("HH_L5_std", DoubleType(), True),
    StructField("HH_L5_magnitude", DoubleType(), True),
    StructField("HH_L5_radius", DoubleType(), True),
    StructField("HH_L5_covariance", DoubleType(), True),
    StructField("HH_L5_pcc", DoubleType(), True),
    StructField("HH_L3_weight", DoubleType(), True),
    StructField("HH_L3_mean", DoubleType(), True),
    StructField("HH_L3_std", DoubleType(), True),
    StructField("HH_L3_magnitude", DoubleType(), True),
    StructField("HH_L3_radius", DoubleType(), True),
    StructField("HH_L3_covariance", DoubleType(), True),
    StructField("HH_L3_pcc", DoubleType(), True),
    StructField("HH_L1_weight", DoubleType(), True),
    StructField("HH_L1_mean", DoubleType(), True),
    StructField("HH_L1_std", DoubleType(), True),
    StructField("HH_L1_magnitude", DoubleType(), True),
    StructField("HH_L1_radius", DoubleType(), True),
    StructField("HH_L1_covariance", DoubleType(), True),
    StructField("HH_L1_pcc", DoubleType(), True),
    StructField("HH_L0_1_weight", DoubleType(), True),
    StructField("HH_L0_1_mean", DoubleType(), True),
    StructField("HH_L0_1_std", DoubleType(), True),
    StructField("HH_L0_1_magnitude", DoubleType(), True),
    StructField("HH_L0_1_radius", DoubleType(), True),
    StructField("HH_L0_1_covariance", DoubleType(), True),
    StructField("HH_L0_1_pcc", DoubleType(), True),
    StructField("HH_L0_01_weight", DoubleType(), True),
    StructField("HH_L0_01_mean", DoubleType(), True),
    StructField("HH_L0_01_std", DoubleType(), True),
    StructField("HH_L0_01_magnitude", DoubleType(), True),
    StructField("HH_L0_01_radius", DoubleType(), True),
    StructField("HH_L0_01_covariance", DoubleType(), True),
    StructField("HH_L0_01_pcc", DoubleType(), True),
    StructField("HH_jit_L5_weight", DoubleType(), True),
    StructField("HH_jit_L5_mean", DoubleType(), True),
    StructField("HH_jit_L5_variance", DoubleType(), True),
    StructField("HH_jit_L3_weight", DoubleType(), True),
    StructField("HH_jit_L3_mean", DoubleType(), True),
    StructField("HH_jit_L3_variance", DoubleType(), True),
    StructField("HH_jit_L1_weight", DoubleType(), True),
    StructField("HH_jit_L1_mean", DoubleType(), True),
    StructField("HH_jit_L1_variance", DoubleType(), True),
    StructField("HH_jit_L0_1_weight", DoubleType(), True),
    StructField("HH_jit_L0_1_mean", DoubleType(), True),
    StructField("HH_jit_L0_1_variance", DoubleType(), True),
    StructField("HH_jit_L0_01_weight", DoubleType(), True),
    StructField("HH_jit_L0_01_mean", DoubleType(), True),
    StructField("HH_jit_L0_01_variance", DoubleType(), True),
    StructField("HpHp_L5_weight", DoubleType(), True),
    StructField("HpHp_L5_mean", DoubleType(), True),
    StructField("HpHp_L5_std", DoubleType(), True),
    StructField("HpHp_L5_magnitude", DoubleType(), True),
    StructField("HpHp_L5_radius", DoubleType(), True),
    StructField("HpHp_L5_covariance", DoubleType(), True),
    StructField("HpHp_L5_pcc", DoubleType(), True),
    StructField("HpHp_L3_weight", DoubleType(), True),
    StructField("HpHp_L3_mean", DoubleType(), True),
    StructField("HpHp_L3_std", DoubleType(), True),
    StructField("HpHp_L3_magnitude", DoubleType(), True),
    StructField("HpHp_L3_radius", DoubleType(), True),
    StructField("HpHp_L3_covariance", DoubleType(), True),
    StructField("HpHp_L3_pcc", DoubleType(), True),
    StructField("HpHp_L1_weight", DoubleType(), True),
    StructField("HpHp_L1_mean", DoubleType(), True),
    StructField("HpHp_L1_std", DoubleType(), True),
    StructField("HpHp_L1_magnitude", DoubleType(), True),
    StructField("HpHp_L1_radius", DoubleType(), True),
    StructField("HpHp_L1_covariance", DoubleType(), True),
    StructField("HpHp_L1_pcc", DoubleType(), True),
    StructField("HpHp_L0_1_weight", DoubleType(), True),
    StructField("HpHp_L0_1_mean", DoubleType(), True),
    StructField("HpHp_L0_1_std", DoubleType(), True),
    StructField("HpHp_L0_1_magnitude", DoubleType(), True),
    StructField("HpHp_L0_1_radius", DoubleType(), True),
    StructField("HpHp_L0_1_covariance", DoubleType(), True),
    StructField("HpHp_L0_1_pcc", DoubleType(), True),
    StructField("HpHp_L0_01_weight", DoubleType(), True),
    StructField("HpHp_L0_01_mean", DoubleType(), True),
    StructField("HpHp_L0_01_std", DoubleType(), True),
    StructField("HpHp_L0_01_magnitude", DoubleType(), True),
    StructField("HpHp_L0_01_radius", DoubleType(), True),
    StructField("HpHp_L0_01_covariance", DoubleType(), True),
    StructField("HpHp_L0_01_pcc", DoubleType(), True)
  ])

len(schema)

df = spark.read.csv(list1,
                  header=False,
                  schema=schema
                 )

df.printSchema()

df = df.na.drop()

from  pyspark.sql.functions import input_file_name

df = df.withColumn("path", input_file_name())


from pyspark.sql.functions import col, udf

def getType(path):
  sampleType = path.split('/')[-1].split('.')[1:-1]
  return "_".join(sampleType)

getTypeUDF = udf(lambda x:getType(x),StringType())
df = df.withColumn("type", getTypeUDF(col("path")))

df = df.drop("path")

df = df.filter("type != ''")

tmp_label_col_name = "type"

print(df.show(5))

features_list = list(df.columns)
features_list.remove(tmp_label_col_name)



feature_col_name = "selectedFeatures"
label_col_name = "label_index"

train_df, test_df = df.randomSplit([.7, .3], seed = 21)

# print(df.groupBy("type").count().show())

# Sampling n records from each label
n = 510000
seed = 21

fractions = train_df.groupBy("type").count().withColumn("required_n", n/col("count"))\
                .drop("count").rdd.collectAsMap()

train_df = train_df.stat.sampleBy("type", fractions, seed)
print(train_df.groupBy("type").count().show())

preprocessing_pipeline = []

from pyspark.ml.feature import StandardScaler, VectorAssembler

unscaled_assembler = VectorAssembler(inputCols=features_list, outputCol="unscaled_features")
scaler = StandardScaler(inputCol="unscaled_features", outputCol="scaled_features")
assembler = VectorAssembler(inputCols=["scaled_features"], outputCol="features")

preprocessing_pipeline += [unscaled_assembler, scaler, assembler]

from pyspark.ml.feature import StringIndexer


label_encoder =  StringIndexer(inputCol=tmp_label_col_name, outputCol=label_col_name)

preprocessing_pipeline += [label_encoder]

from pyspark.ml.feature import UnivariateFeatureSelector

feature_selector = UnivariateFeatureSelector(featuresCol="features", outputCol=feature_col_name,
                                     labelCol="label_index", selectionMode="numTopFeatures")

feature_selector.setFeatureType("continuous").setLabelType("continuous").setSelectionThreshold(60)

preprocessing_pipeline += [feature_selector]


from pyspark.ml import Pipeline

print()
print("Preprocessing the Dataset: ")

# Set Pipeline
preprocessing_pipeline = Pipeline(stages=preprocessing_pipeline)

# Fit Pipeline to Data
preprocessing_pipeline = preprocessing_pipeline.fit(train_df)

# Transform Data using Fitted Pipeline
train_df = preprocessing_pipeline.transform(train_df)
test_df = preprocessing_pipeline.transform(test_df)

print(train_df.limit(5).toPandas())

# Number of Classes in Train Data
number_of_classes = train_df.select("label_index").distinct().count()

# Number of features in Train Data
input_dimension = len(train_df.select(feature_col_name).first()[0])
print("\nIn Train Data: ")
print("number_of_classes: ", number_of_classes)
print("input_dimension: ", input_dimension)

# Number of Classes in Test Data
number_of_classes = test_df.select("label_index").distinct().count()

# Number of features in Test Data
input_dimension = len(test_df.select(feature_col_name).first()[0])
print("\nIn Test Data: ")
print("number_of_classes: ", number_of_classes)
print("input_dimension: ", input_dimension)

from pyspark.sql.functions import rand

train_df = train_df.select('selectedFeatures', 'label_index')
train_df = train_df.orderBy(rand())

test_df = test_df.select('selectedFeatures', 'label_index')

"""Training the Model"""

print("\nLR Model: ")

from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(featuresCol = 'selectedFeatures',labelCol = 'label_index', maxIter=20, regParam=0.3, elasticNetParam=0)

from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[lr])

model = pipeline.fit(train_df)

prediction = model.transform(train_df)


from pyspark.ml.evaluation import MulticlassClassificationEvaluator
binEval = MulticlassClassificationEvaluator().setMetricName("accuracy").setPredictionCol("prediction").setLabelCol("label_index")

print("Training Accuracy: ")
print(binEval.evaluate(prediction))

test_prediction = model.transform(test_df)

print("\nTest Data Predictions: ")
print(test_prediction.show())

from pyspark.mllib.evaluation import MulticlassMetrics

pnl_train = prediction.select("label_index", "prediction")
pnl_test = test_prediction.select("label_index", "prediction")

pred_and_label_train = pnl_train.rdd.map(lambda row: (row["label_index"], row['prediction']))
pred_and_label_test = pnl_test.rdd.map(lambda row: (row["label_index"], row['prediction']))

metrics_train = MulticlassMetrics(pred_and_label_train)
metrics_test = MulticlassMetrics(pred_and_label_test)

np.set_printoptions(suppress=True)

print("\nTrain Confusion Matrix: ")

metrics_train.confusionMatrix().toArray()
cm = metrics_train.confusionMatrix().toArray()

numpy_cm = np.array(cm)
print(numpy_cm)


binEval = MulticlassClassificationEvaluator().setMetricName("accuracy").setPredictionCol("prediction").setLabelCol("label_index")

eval_precision = MulticlassClassificationEvaluator(labelCol="label_index", predictionCol="prediction", metricName="precisionByLabel")
eval_recall = MulticlassClassificationEvaluator(labelCol="label_index", predictionCol="prediction", metricName="recallByLabel")
eval_f1 = MulticlassClassificationEvaluator(labelCol="label_index", predictionCol="prediction", metricName="f1")

print("Test Accuracy: " + str(binEval.evaluate(test_prediction)))

print("Test Precision: " + str(eval_precision.evaluate(test_prediction)))
print("Test Recall: " + str(eval_recall.evaluate(test_prediction)))
print("Test F1 Score: " + str(eval_f1.evaluate(test_prediction)))

print("\nTest Confusion Matrix: ")

metrics_test.confusionMatrix().toArray()
cm_test = metrics_test.confusionMatrix().toArray()

numpy_cm_test = np.array(cm_test)
print(numpy_cm_test)


print("\n RF Model: ")
from pyspark.ml.classification import RandomForestClassifier

rf = RandomForestClassifier(labelCol="label_index", featuresCol="selectedFeatures", numTrees=10)

pipeline = Pipeline(stages=[rf])

model = pipeline.fit(train_df)
prediction = model.transform(train_df)


binEval = MulticlassClassificationEvaluator().setMetricName("accuracy").setPredictionCol("prediction").setLabelCol("label_index")


print("Training Accuracy: ")
print(binEval.evaluate(prediction))

test_prediction = model.transform(test_df)

print("\nTest Data Predictions: ")
print(test_prediction.show())


pnl_train = prediction.select("label_index", "prediction")
pnl_test = test_prediction.select("label_index", "prediction")

pred_and_label_train = pnl_train.rdd.map(lambda row: (row["label_index"], row['prediction']))
pred_and_label_test = pnl_test.rdd.map(lambda row: (row["label_index"], row['prediction']))

metrics_train = MulticlassMetrics(pred_and_label_train)
metrics_test = MulticlassMetrics(pred_and_label_test)

print("\nTrain Confusion Matrix: ")

metrics_train.confusionMatrix().toArray()
cm = metrics_train.confusionMatrix().toArray()

numpy_cm = np.array(cm)
print(numpy_cm)


binEval = MulticlassClassificationEvaluator().setMetricName("accuracy").setPredictionCol("prediction").setLabelCol("label_index")

eval_precision = MulticlassClassificationEvaluator(labelCol="label_index", predictionCol="prediction", metricName="precisionByLabel")
eval_recall = MulticlassClassificationEvaluator(labelCol="label_index", predictionCol="prediction", metricName="recallByLabel")
eval_f1 = MulticlassClassificationEvaluator(labelCol="label_index", predictionCol="prediction", metricName="f1")

print("Test Accuracy: " + str(binEval.evaluate(test_prediction)))

print("Test Precision: " + str(eval_precision.evaluate(test_prediction)))
print("Test Recall: " + str(eval_recall.evaluate(test_prediction)))
print("Test F1 Score: " + str(eval_f1.evaluate(test_prediction)))

print("\nTest Confusion Matrix: ")

metrics_test.confusionMatrix().toArray()
cm_test = metrics_test.confusionMatrix().toArray()

numpy_cm_test = np.array(cm_test)
print(numpy_cm_test)



print("\n DT Model: ")
from pyspark.ml.classification import DecisionTreeClassifier

dt = DecisionTreeClassifier(labelCol="label_index", featuresCol="selectedFeatures")

pipeline = Pipeline(stages=[dt])

model = pipeline.fit(train_df)
prediction = model.transform(train_df)



binEval = MulticlassClassificationEvaluator().setMetricName("accuracy").setPredictionCol("prediction").setLabelCol("label_index")

print("Training Accuracy: ")
print(binEval.evaluate(prediction))

test_prediction = model.transform(test_df)

print("\nTest Data Predictions: ")
print(test_prediction.show())


pnl_train = prediction.select("label_index", "prediction")
pnl_test = test_prediction.select("label_index", "prediction")

pred_and_label_train = pnl_train.rdd.map(lambda row: (row["label_index"], row['prediction']))
pred_and_label_test = pnl_test.rdd.map(lambda row: (row["label_index"], row['prediction']))


metrics_train = MulticlassMetrics(pred_and_label_train)
metrics_test = MulticlassMetrics(pred_and_label_test)

print("\nTrain Confusion Matrix: ")

metrics_train.confusionMatrix().toArray()
cm = metrics_train.confusionMatrix().toArray()

numpy_cm = np.array(cm)
print(numpy_cm)
print()


binEval = MulticlassClassificationEvaluator().setMetricName("accuracy").setPredictionCol("prediction").setLabelCol("label_index")

eval_precision = MulticlassClassificationEvaluator(labelCol="label_index", predictionCol="prediction", metricName="precisionByLabel")
eval_recall = MulticlassClassificationEvaluator(labelCol="label_index", predictionCol="prediction", metricName="recallByLabel")
eval_f1 = MulticlassClassificationEvaluator(labelCol="label_index", predictionCol="prediction", metricName="f1")

print("Test Accuracy: " + str(binEval.evaluate(test_prediction)))

print("Test Precision: " + str(eval_precision.evaluate(test_prediction)))
print("Test Recall: " + str(eval_recall.evaluate(test_prediction)))
print("Test F1 Score: " + str(eval_f1.evaluate(test_prediction)))


print("\nTest Confusion Matrix: ")

metrics_test.confusionMatrix().toArray()
cm_test = metrics_test.confusionMatrix().toArray()

numpy_cm_test = np.array(cm_test)
print(numpy_cm_test)



spark.stop()
