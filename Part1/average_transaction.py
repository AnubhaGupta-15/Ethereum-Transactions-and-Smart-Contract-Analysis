from mrjob.job import MRJob
import time
class average(MRJob):

    def mapper(self, _, line):
        try:
            l=line.split(',')
            timestamp=int(l[6])
            tranv=float(l[3])/1000000000000000000
            ym=time.strftime('%Y-%m',time.gmtime(timestamp))
            yield (ym,{'count':1,'trxn':tranv})
        except:
             pass

    def combiner(self, key, value):
        val=0.0
        c=0
        for v in value:
            c=c+v['count']
            val=val+v['trxn']
        result={'count':c,'trxn':val}

        yield (key,result)

    def reducer(self, key, value):
        val=0.0
        c=0
        for v in value:
            c=c+v['count']

            val=val+v['trxn']


        average=val/c
        yield(key,average)

if __name__=='__main__':
    average.JOBCONF={'mapreduce.job.reduces':'5'}
    average.run()
