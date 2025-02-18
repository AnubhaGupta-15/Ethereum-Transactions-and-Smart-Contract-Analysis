from mrjob.job import MRJob
import time

class Gas(MRJob):

    def mapper(self,_,line):
        try:
            fields = line.split(',')
            gas = float(fields[4])
            date  = time.localtime(float(fields[6]))
            if len(fields) == 7:
                yield ((date.tm_mon,date.tm_year),(1,gas))   

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
    Gas.run()