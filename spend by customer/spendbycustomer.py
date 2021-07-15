from mrjob.job  import MRJob

class SpendByCustomer(MRJob):

    def mapper(self,_,line):
        (customer,item,order_amount) = line.split(",")
        yield customer,float(order_amount)

    def reducer(self,customer,amount):
        yield customer,sum(amount)

    

if __name__ == '__main__':
    SpendByCustomer.run()

