from mrjob.job import MRJob
from mrjob.step import MRStep

class top_services(MRJob):
		def mapper1(self, _, line):
			fields = line.split(',')
			try:
				if len(fields) == 7:
					taddr = fields[2]
					value = int(fields[3])
					yield (taddr, (1,value))
				elif len(fields) == 5:
					caddr = fields[0]
					yield (caddr, (0,0))
			except:
				pass
		def reducer1(self, key, values):
			f = False
			c = 0
			for i in values:
				if i[0]==1:
					c=c+i[1]
				elif i[0] == 0:
					f = True
			if f:
				yield(key,c)

		def mapper2(self, key,value):
			yield (None, (key,value))

		def reducer2(self, _, values):
			sorted_values = sorted(values, reverse = True, key = lambda values: values[1])
			j=0

			for i in sorted_values:
				yield i[0], i[1]
				j=j+1
				if j>=10:
					break

		def steps(self):
			return [MRStep(mapper = self.mapper1, reducer=self.reducer1), MRStep(mapper = self.mapper2, reducer = self.reducer2)]

if __name__ == '__main__':
		top_services.run()
