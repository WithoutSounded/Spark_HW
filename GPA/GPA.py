from pyspark.sql import SparkSession

def grade_mapping(x):
    if x == 'A':
        return 4
    elif x == 'B':
        return 3
    elif x == 'C':
        return 2
    elif x == 'D':
        return 1
    elif x == 'E':
        return 0
    else:
        return 0
        
def sep_g(tx):
    tx = tx.split(' ')
    _list = []
    for i in range(len(tx)):
        if i%2:
            _list.append((tx[i-1],grade_mapping(tx[i])))
    return _list

def sep(tx):
    classID, grade = tx.split(' ')
    return (classID, int(grade))

def counting(tx):
    # tx[0]: class
    # tx[1]: (credit, grade)
    grade = tx[1][1]
    credit = tx[1][0]
    return (grade*credit, credit)


spark = SparkSession\
    .builder\
    .appName("GPA_cal")\
    .getOrCreate()
sc = spark.sparkContext


student_info_path = 'file:///home/pcdm/BigDataSystem/sparkHW/grade_reports.txt'
class_info_path = 'file:///home/pcdm/BigDataSystem/sparkHW/class_credits.txt'


student_info = sc.textFile(student_info_path)
class_info = sc.textFile(class_info_path)

pre_st = student_info.flatMap(lambda line: line.split(',')).collect()[1]

stud_grade = sc.parallelize(sep_g(pre_st))

tx = class_info.flatMap(lambda x: x.split(',')).map(sep)

total_flat = tx.join(stud_grade)

result = total_flat.map(counting).collect()


total_grade=0
total_credit=0
for i in result:
    total_grade+=i[0]
    total_credit+=i[1]

print('*'*40)
print("THE GPA IS ",total_grade/total_credit)
print('*'*40)





