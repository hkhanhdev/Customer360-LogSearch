import os,datetime as dt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when,concat_ws, to_date, date_format,lower,monotonically_increasing_id,lit
from pathlib import Path

# Initialize SparkSession
spark = SparkSession.builder.appName("ReadParquet").getOrCreate()

source_path = "D:\\Personal_Doc\\study_DE\\BigData\\log_search\\"
csv_output_path = "D:\\Personal_Doc\\Class7\\"

def getListDir():
	# Get all files in the directory
	folders = os.listdir(source_path)
	# Filter files based on date range
	t6folders = {}
	t7folders = {}

	for folder in folders:
		file_path = os.path.join(source_path,folder)

		folder_month = folder[4:6]
		files = []
		for file in os.listdir(file_path):
			if file.endswith('.parquet') and folder_month == '06':
				files.append(file)
			elif file.endswith('.parquet') and folder_month == '07':
				files.append(file)
		if folder_month == '06':
			t6folders[folder] = files
		elif folder_month == '07':
			t7folders[folder] = files
	return t6folders,t7folders

def renamePQFile(folder):
	"""Nếu trong folder nào chứa nhiều partitions parquet thì script này 
	sẽ chỉ đổi tên file.parquet đầu tiên nó tìm được. Vì trùng tên"""
	try:
		file_path = os.path.join(source_path,folder)
		for file in os.listdir(file_path):
			if file.endswith('.parquet'):
				old_file_path = os.path.join(file_path, file)
				# New file name matches folder name with .parquet extension
				new_file_name = f"{folder}.parquet"
				new_file_path = os.path.join(file_path, new_file_name)
				# Rename the file
				os.rename(old_file_path, new_file_path)
				print(f"Renamed {file} to {new_file_name} in folder {folder}")
	except Exception as e:
		log.error(f"Error when renaming file: {e}")
	
def readParquet(path):
	try:
		print("-----------------READING PARQUET------------------")
		df = spark.read.parquet(path)
		print("---------------READING SUCCESSFULLY---------------")
		return df
	except Exception as e:
		print(f"Error while reading parquet file: {e}")
		return False

def readCSV(path):
	try:
		print("-----------------READING CSV------------------")
		df = (spark.read.option("header", "true").option("encoding", "UTF-8").csv(path))
		print("---------------READING SUCCESSFULLY---------------")
		return df
	except Exception as e:
		print(f"Error while reading csv file: {e}")
		return False

def writeCSV(df_final,where):
	try:
		print("--------------SAVING RESULT-------------")
		# log.info("--------------SAVING RESULT-------------")
		df_final.coalesce(1).write.mode("overwrite") \
		.option("header", "true") \
		.option("encoding", "UTF-8").csv(f"{csv_output_path}{where}",header=True)
		# log.info("---------------------------------------")
		print("---------------------------------------")
	except Exception as e:
		print(f"Getting error when saving result: {e}")
		return False

def processingT6(t6_dataset):
	union_result = None 
	for folder,files in t6_dataset.items():
		for file_name in files:
			df = readParquet(os.path.join(source_path,folder,file_name))
			#Convert col with array<String> type to String type
			result = df.withColumn("userPlansMap", concat_ws(",", col("userPlansMap")))
			# Step 1: Filter out rows where category == 'quit'
			df_filtered = result.filter(col("category") != "quit")
			# Step 2: Drop the 'category' and 'action' columns
			df_filtered = df_filtered.drop("category", "action")
			# Step 3: Split 'datetime' into 'date' and 'time'
			df_filtered = df_filtered.withColumn("date", to_date(col("datetime"))) 
			df_filtered = df_filtered.withColumn("month", date_format(col("date"), "MM"))
			# Step 4: Remove outlier
			df_filtered = df_filtered.filter((col('month').isNotNull()) & (col('month') == 6)) 
			# # Check for rows with NULL month or time -> resulted in col datetime is stored invalid dt format
			# null_month = df_filtered.filter(col("month").isNull())
			# null_month_count = null_month.count()
			# if null_month_count > 0:
			#     print(f"File {file_name} has {null_month_count} rows with NULL month")
			#     null_month.show(1, truncate=False)
			# outlier = df_filtered.filter(col("month") != 6)
			# outlier_count = outlier.count()
			# if outlier_count > 0:
			#     print(f"File {file_name} has {outlier_count} rows with month value != 6")
			#     outlier.show(1, truncate=False)

			# Step 5: Normalize 'keyword' to lowercase
			df_normalized = df_filtered.withColumn("keyword_normalized", lower(col("keyword")))
			# Bước 6: Nhóm theo keyword, date, category và đếm số lượng tìm kiếm
			search_counts = df_normalized.groupBy("keyword_normalized", "date",'month').count().withColumnRenamed('count','searches').withColumnRenamed('keyword_normalized','keyword')
			search_counts = search_counts.orderBy(col('searches').desc())
			# writeCSV(search_counts)
			if union_result == None:
				union_result = search_counts
			else:
				union_result = union_result.union(search_counts)

	df_final = union_result.groupBy("keyword",'month').sum().withColumnRenamed('sum(searches)','searches').orderBy(col('searches').desc())
	return df_final                

def processingT7(t7_dataset):
	union_result = None 
	for folder,files in t7_dataset.items():
		for file_name in files:
			df = readParquet(os.path.join(source_path,folder,file_name))
			#Convert col with array<String> type to String type
			result = df.withColumn("userPlansMap", concat_ws(",", col("userPlansMap")))
			# Step 1: Filter out rows where category == 'quit'
			df_filtered = result.filter(col("category") != "quit")
			# Step 2: Drop the 'category' and 'action' columns
			df_filtered = df_filtered.drop("category", "action")
			# Step 3: Split 'datetime' into 'date' and 'time'
			df_filtered = df_filtered.withColumn("date", to_date(col("datetime"))) 
			df_filtered = df_filtered.withColumn("month", date_format(col("date"), "MM"))
			# Step 4: Remove outlier
			df_filtered = df_filtered.filter((col('month').isNotNull()) & (col('month') == 7)) 
			# # Check for rows with NULL month or time -> resulted in col datetime is stored invalid dt format
			# null_month = df_filtered.filter(col("month").isNull())
			# null_month_count = null_month.count()
			# if null_month_count > 0:
			#     print(f"File {file_name} has {null_month_count} rows with NULL month")
			#     null_month.show(1, truncate=False)
			# outlier = df_filtered.filter(col("month") != 6)
			# outlier_count = outlier.count()
			# if outlier_count > 0:
			#     print(f"File {file_name} has {outlier_count} rows with month value != 6")
			#     outlier.show(1, truncate=False)

			# Step 5: Normalize 'keyword' to lowercase
			df_normalized = df_filtered.withColumn("keyword_normalized", lower(col("keyword")))
			# Bước 6: Nhóm theo keyword, date, category và đếm số lượng tìm kiếm
			search_counts = df_normalized.groupBy("keyword_normalized", "date",'month').count().withColumnRenamed('count','searches').withColumnRenamed('keyword_normalized','keyword')
			search_counts = search_counts.orderBy(col('searches').desc())
			# writeCSV(search_counts)
			if union_result == None:
				union_result = search_counts
			else:
				union_result = union_result.union(search_counts)

	df_final = union_result.groupBy("keyword",'month').sum().withColumnRenamed('sum(searches)','searches').orderBy(col('searches').desc())
	return df_final  

def combiningResults():
	t6_output_path = "D:\\Personal_Doc\\Class7\\result_t6\\result_t6.csv"
	t7_output_path = "D:\\Personal_Doc\\Class7\\result_t7\\result_t7.csv"
	# Bước 1: Đọc 2 file CSV
	df1 = spark.read.csv(t6_output_path, header=True, inferSchema=True)
	df2 = spark.read.csv(t7_output_path, header=True, inferSchema=True)
	# Bước 2: Đổi tên cột để phân biệt giữa 2 DataFrame
	df1 = df1.withColumnRenamed("keyword", "keyword_1") \
	 .withColumnRenamed("month", "month_1") \
	 .withColumnRenamed("searches", "searches_1")

	df2 = df2.withColumnRenamed("keyword", "keyword_2") \
	 .withColumnRenamed("month", "month_2") \
	 .withColumnRenamed("searches", "searches_2")

	# Bước 3: Thêm cột chỉ số (index) để ghép theo dòng
	df1 = df1.withColumn("index", monotonically_increasing_id())
	df2 = df2.withColumn("index", monotonically_increasing_id())

	# Bước 4: Kết hợp 2 DataFrame theo cột index
	combined_df = df1.join(df2, "index", "inner")

	# Bước 5: Chọn top 100 bản ghi và bỏ cột index
	result_df = combined_df.drop("index").limit(100)

	# # Bước 6: Hiển thị kết quả
	# result_df.show(100, truncate=False)
	writeCSV(result_df,'combined_results_uncategorized')

def main():
	# #Calculating
	# t6,t7 = getListDir()
	# t6 = processingT6(t6)
	# t7 = processingT7(t7)
	# writeCSV(t6,'result_t6')
	# writeCSV(t7,'result_t7')
	csv_path = "D:\\Personal_Doc\\Class7\\result_categorized.csv"
	df = readCSV(csv_path)
	df = df.withColumn(
	"trending_type",
	when(col("keyword categorized_1") == col("keyword categorized_2"), "unchanged").otherwise("changed")
	)

	df = df.withColumn(
	"changes",
	when(col("trending_type") == "changed",
	     concat_ws(" | ", col("keyword categorized_1"), col("keyword categorized_2"))
	).otherwise(lit("unchanged"))
	)
	df = df.select("keyword_1",'keyword categorized_1','keyword_2','keyword categorized_2','trending_type','changes')
	writeCSV(df,'final_result')
	# df.show(10)

if __name__ == '__main__':
	try:
		main()
	except Exception as e:
		print(f"Getting error while running main: {e}")
	else:
		print("SUCCESSFULLY!")
	finally:
		spark.stop()