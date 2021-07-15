from mrjob.job  import MRJob
from mrjob.step import MRStep
import re

class SpendByCustomer(MRJob):

    def steps(self):
        return[
            MRStep(mapper=self.mapper_get_amount,
                   reducer=self.reducer_sum_amount),
            MRStep(mapper=self.mapper_sort_by_spend,
                   reducer=self.reducer_output_spend)
        ]

    def mapper_get_amount(self,_,line):
        (customer,item,order_amount) = line.split(",")
        yield customer,float(order_amount)

    def reducer_sum_amount(self,customer,spend):
        yield customer,sum(spend)

    def mapper_sort_by_spend(self,customer,spend):
        yield '%04.02f'%float(spend), customer

    def reducer_output_spend(self,spend,customers):
        for customer in customers:
            yield customer,spend

    

if __name__ == '__main__':
    SpendByCustomer.run()

