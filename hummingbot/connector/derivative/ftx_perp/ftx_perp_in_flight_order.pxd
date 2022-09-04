from hummingbot.connector.in_flight_order_base cimport InFlightOrderBase

cdef class FtxPerpInFlightOrder(InFlightOrderBase):
    cdef:
        public double created_at
        public object state
        public object leverage
        public object position
