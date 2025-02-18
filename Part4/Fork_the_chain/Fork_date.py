from mrjob.job import MRJob
import time

class block(MRJob):
	def mapper(self,_,line):
		try:
			fields = line.split(',')
			block_num = float(fields[0])
			miner=fields[2]
			date=time.strftime('%Y-%m', time.gmtime(float(fields[7])))
			yield (block_num,(miner,date))
		except:
	            pass
	def reducer(self,key,value):
		c=0
		for i in value:
			if i[0]:
				c=c+1
				d=i[1]
		if c>=2:
			yield (None,d)


if __name__=='__main__':
	block.run()
