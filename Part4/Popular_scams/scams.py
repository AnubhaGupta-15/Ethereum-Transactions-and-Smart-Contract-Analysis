from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class scams(MRJob):
	def mapper1(self, _, lines):
		try:
			fields = lines.split(",")
			if len(fields) == 7:
				trxnadd = fields[2]
				yield (trxnadd, (1,0))
			else:
				line = json.loads(lines)
				result = line["result"]
				for i in result:
					records = line["result"][i]
					category = records["category"]
					addresses = records["addresses"]
					status = records["status"]
					for j in addresses:
						yield (j, (2, category,status))
		except:
			pass
	def reducer1(self, key, values):
		f=0
		category=None
		status = None
		for i in values:
			if i[0] == 1:
				f = f + i[0]
			else:
				category = i[1]
				status = i[2]
		if category is not None and status is not None:
			yield ((category,status), f)

	def mapper2(self,key,value):
		yield(key,value)

	def reducer2(self, key, value):
		yield(key,sum(value))


	def steps(self):
		return [MRStep(mapper = self.mapper1, reducer=self.reducer1), MRStep(mapper = self.mapper2, reducer = self.reducer2)]

if __name__ == '__main__':
	scams.run()
