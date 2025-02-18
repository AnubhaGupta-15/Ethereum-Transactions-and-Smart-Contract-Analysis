import pyspark
from operator import add
sc = pyspark.SparkContext()

def is_good_line(line):
    try:
        fields = line.split(',')
        if len(fields) == 7: 
            str(fields[2]) 
            if int(fields[3]) == 0:
                return False
        elif len(fields) == 5:
            str(fields[0]) 
        else:
            return False
        return True
    except:
        return False

transactions = sc.textFile('/data/ethereum/transactions').filter(is_good_line)
contracts = sc.textFile('/data/ethereum/contracts').filter(is_good_line)


trxn = transactions.map(lambda i: (i.split(',')[2], int(i.split(',')[3])))
second= trxn.reduceByKey(add)
third = second.join(contracts.map(lambda x: (x.split(',')[0], 'contract')))
smart_contract = third.takeOrdered(10, key = lambda x: -x[1][0])

for record in smart_contract:
    print("{},{}".format(record[0], int(record[1][0]/1000000000000000000)))