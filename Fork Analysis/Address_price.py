from mrjob.job import MRJob
import time
class block(MRJob):
	def mapper(self,_,line):
		try:
			fields = line.split(',')
			price= float(fields[5])
			add=fields[2]
			date=time.gmtime(float(fields[6]))
			if (date.tm_year == 2017 and date.tm_mon == 12):
				yield (None,(add,price))
		except:
			pass


	def reducer(self,key,value):
		sort=sorted(value,reverse=True,key=lambda value: value[1])
		for i in sort:
			yield(i[0],i[1])


if __name__=='__main__':
	    block.run()
