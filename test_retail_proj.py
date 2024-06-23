import pytest
from lib.DataReader import read_customers,read_orders
from lib.DataManipulation import filter_closed_orders,filter_orders_generic


def test_count_customers(spark):
    customers_count = read_customers(spark,"LOCAL").count()
    assert customers_count == 12435
    
def test_count_orders(spark):
    orders_count = read_orders(spark,"LOCAL").count()
    assert orders_count == 68884
    
@pytest.mark.transformation()
def test_filter_closed_orders(spark):
    orders_df = read_orders(spark,"LOCAL")
    filtered_count = filter_closed_orders(orders_df).count()
    assert filtered_count == 7556
    
@pytest.mark.skip()
def test_check_closed_count(spark):
    orders_df = read_orders(spark,"LOCAL")
    filtered_count = filter_orders_generic(orders_df,"CLOSED").count()
    assert filtered_count == 7556
    
@pytest.mark.skip() 
def test_check_complete_count(spark):
    orders_df = read_orders(spark,"LOCAL")
    filtered_count = filter_orders_generic(orders_df,"COMPLETE").count()
    assert filtered_count == 22900
    
@pytest.mark.skip()  
def test_check_pendingpayment_count(spark):
    orders_df = read_orders(spark,"LOCAL")
    filtered_count = filter_orders_generic(orders_df,"PENDING_PAYMENT").count()
    assert filtered_count == 15030
    
@pytest.mark.parametrize(
    "status,count",
    [
     ("CLOSED",7556),
     ("COMPLETE",22900),
     ("PENDING_PAYMENT",15030)
     ]
)
def test_check_generic_count(spark,status,count):
    orders_df = read_orders(spark,"LOCAL")
    filtered_count = filter_orders_generic(orders_df,status).count()
    assert filtered_count == count
    