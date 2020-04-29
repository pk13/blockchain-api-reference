class OrderObject:
    symbol = ''
    ordType = ''
    side = ''
    orderQty = 0.0
    price = 0.0

def construct_order_object(symbol, ordType, side, orderQty, price):
    order = OrderObject()
    order.symbol = symbol
    order.side = side
    order.ordType = ordType
    order.orderQty = orderQty
    order.price = price
    return order
