from mrjob.job import MRJob
import time

class Gas_price(MRJob):

    def mapper(self,_,line):
        try:
            fields = line.split(',')
            price= float(fields[5])
            date  = time.localtime(float(fields[6]))
            if len(fields) == 7:
                yield ((date.tm_mon,date.tm_year),(1,price))    #Number of transaction

        except:
            pass

    def combiner(self,key,value):
        count = 0
        total = 0
        for v in value:
            count+=v[0]
            total+=v[1]
        yield (key,(count,total))

    def reducer(self,key,value):
        count = 0
        total = 0
        for v in value:
            count+=v[0]
            total+=v[1]
        yield (key,(total/count))


if __name__=='__main__':
    Gas_price.run()