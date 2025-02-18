from mrjob.job import MRJob
from mrjob.step import MRStep

class top_active_miners(MRJob):
    def steps(self):
        return [MRStep(mapper = self.mapper, reducer=self.reducer), MRStep(mapper = self.mapper2, reducer = self.reducer2)]


    def mapper(self,_,line):
        try:
            fields = line.split(',')
            miner_address = fields[2]
            size= int(fields[4])
            yield (miner_address,size)

        except:
            pass
    def reducer(self, key, value):

        yield (key,sum(value))

    def mapper2(self,key,value):
        try:
            yield(None,(key,value))
        except:
            pass
    def reducer2(self,_ ,values):
        sorted_values = sorted(values, reverse = True, key = lambda values: values[1])
        i=0
        for j in sorted_values:
            yield(j[0], j[1])
            i=i+1
            if i>=10:
                break



if __name__ == '__main__':
		top_active_miners.run()
