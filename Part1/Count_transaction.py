from mrjob.job import MRJob
import time

class count(MRJob):
    def mapper(self, _, line):
        try:
           fields=line.split(',')
           timestamp=int(fields[6])
           ym=time.strftime('%Y-%m', time.gmtime(timestamp))
           yield (ym,1)
        except:
            pass

    def combiner(self, key, value):
        yield (key, sum(value))

    def reducer(self, key, value):
        yield (key,sum(value))

if __name__ == "__main__":
    count.JOBCONF={'mapreduce.job.reduces':'5'}
    count.run()
