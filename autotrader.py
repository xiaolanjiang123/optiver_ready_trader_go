# Copyright 2021 Optiver Asia Pacific Pty. Ltd.
#
# This file is part of Ready Trader Go.
#
#     Ready Trader Go is free software: you can redistribute it and/or
#     modify it under the terms of the GNU Affero General Public License
#     as published by the Free Software Foundation, either version 3 of
#     the License, or (at your option) any later version.
#
#     Ready Trader Go is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU Affero General Public License for more details.
#
#     You should have received a copy of the GNU Affero General Public
#     License along with Ready Trader Go.  If not, see
#     <https://www.gnu.org/licenses/>.
import asyncio
import itertools
from concurrent.futures import ThreadPoolExecutor

from typing import List

from ready_trader_go import BaseAutoTrader, Instrument, Lifespan, MAXIMUM_ASK, MINIMUM_BID, Side

# Original Config
POSITION_LIMIT = 100
TICK_SIZE_IN_CENTS = 100
MIN_BID_NEAREST_TICK = (MINIMUM_BID + TICK_SIZE_IN_CENTS) // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS
MAX_ASK_NEAREST_TICK = MAXIMUM_ASK // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS

# Custom Config
NUM_THREADS = 15
BASE_LOT_SIZE = 60
FUT_CAP_WEIGHT, ETF_CAP_WEIGHT, TIC_CAP_WEIGHT = 0.4, 0.3, 0.3
class AutoTrader(BaseAutoTrader):
    """Example Auto-trader.

    When it starts this auto-trader places ten-lot bid and ask orders at the
    current best-bid and best-ask prices respectively. Thereafter, if it has
    a long position (it has bought more lots than it has sold) it reduces its
    bid and ask prices. Conversely, if it has a short position (it has sold
    more lots than it has bought) then it increases its bid and ask prices.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop, team_name: str, secret: str):
        """Initialise a new instance of the AutoTrader class."""
        super().__init__(loop, team_name, secret)
        self.thread_pool = ThreadPoolExecutor(max_workers=NUM_THREADS)
        self.order_ids = itertools.count(1)
        self.bids = set()
        self.asks = set()
        self.ask_id = self.ask_price = self.bid_id = self.bid_price = self.position = 0
        
        self.etf_book = {Side.BID: ([],[]), Side.ASK: ([],[])}
        self.fut_book = {Side.BID: ([],[]), Side.ASK: ([],[])}
        self.etf_tick = {Side.BID: ([],[]), Side.ASK: ([],[])}
        self.fut_tick = {Side.BID: ([],[]), Side.ASK: ([],[])}

    def on_error_message(self, client_order_id: int, error_message: bytes) -> None:
        """Called when the exchange detects an error.

        If the error pertains to a particular order, then the client_order_id
        will identify that order, otherwise the client_order_id will be zero.
        """
        self.logger.warning(f"error with order {client_order_id}: {error_message.decode()}")
        if client_order_id != 0 and (client_order_id in self.bids or client_order_id in self.asks):
            self.on_order_status_message(client_order_id, 0, 0, 0)

    def on_hedge_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        """Called when one of your hedge orders is filled.

        The price is the average price at which the order was (partially) filled,
        which may be better than the order's limit price. The volume is
        the number of lots filled at that price.
        """
        self.logger.info(f"received hedge filled for order({client_order_id}) with average price {price} and volume {volume}")

    def on_order_book_update_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                                     ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        """Called periodically to report the status of an order book.

        The sequence number can be used to detect missed or out-of-order
        messages. The five best available ask (i.e. sell) and bid (i.e. buy)
        prices are reported along with the volume available at each of those
        price levels.
        """
        if instrument == Instrument.FUTURE:
            # Update local order book
            self.fut_book[Side.ASK] = (ask_prices, ask_volumes)
            self.fut_book[Side.BID] = (bid_prices, bid_volumes)

            price_adjustment = - (self.position // BASE_LOT_SIZE) * TICK_SIZE_IN_CENTS
            new_bid_price = bid_prices[0] + price_adjustment if bid_prices[0] != 0 else 0
            new_ask_price = ask_prices[0] + price_adjustment if ask_prices[0] != 0 else 0

            # Estimate market inclination and set order size accordingly
            buy_total_cap, sell_total_cap = self.calculate_sides_capital()
            if sell_total_cap == 0 or sell_total_cap == 0:
                return
            buy_sell_ratio = buy_total_cap / sell_total_cap 
            is_buyer_market = buy_sell_ratio > 1.0
            is_seller_market = buy_sell_ratio < 1.0
            target_order_size = int(BASE_LOT_SIZE * abs(buy_sell_ratio-1.0))
            if target_order_size == 0:
                self.logger.info(f"neutral market inclination, no order should be sent")
                return 

            # Cancel any current orders if needed
            # should_cancel_bid = self.bid_id != 0 and new_bid_price not in (self.bid_price, 0)
            # if is_buy_market and should_cancel_bid:
            #     self.send_cancel_order(self.ask_id) # cancel the reverse order 
            #     self.send_cancel_order(self.bid_id) # cancel the current order 
            #     self.bid_id = 0
            # should_cancel_ask = self.ask_id != 0 and new_ask_price not in (self.ask_price, 0)
            # if is_sell_market and should_cancel_ask:
            #     self.send_cancel_order(self.bid_id) # cancel the reverse order 
            #     self.send_cancel_order(self.ask_id) # cancel the current order 
            #     self.ask_id = 0

            # Execute available orders
            should_buy = self.bid_id == 0 and new_bid_price != 0 and self.position < POSITION_LIMIT
            if is_buyer_market and should_buy:
                self.bid_id = next(self.order_ids)
                self.bid_price = new_bid_price
                order_size = min(target_order_size, abs(POSITION_LIMIT-self.position))
                self.thread_pool.submit(self.send_insert_order,self.bid_id, Side.BUY, new_bid_price, order_size, Lifespan.FILL_AND_KILL)
                self.bids.add(self.bid_id)
                self.logger.info(f"sending buy order({self.bid_id}) at {new_bid_price} of size {order_size}")

            should_sell = self.ask_id == 0 and new_ask_price != 0 and self.position > -POSITION_LIMIT
            if is_seller_market and should_sell:
                self.ask_id = next(self.order_ids)
                self.ask_price = new_ask_price
                order_size = min(target_order_size, abs(-POSITION_LIMIT-self.position))
                self.thread_pool.submit(self.send_insert_order, self.ask_id, Side.SELL, new_ask_price, order_size, Lifespan.FILL_AND_KILL)
                self.asks.add(self.ask_id)
                self.logger.info(f"sending sell order({self.ask_id}) at {new_ask_price} of size {order_size}")
            
        elif instrument == Instrument.ETF:
            # Update local order book
            self.etf_book[Side.ASK] = (ask_prices, ask_volumes)
            self.etf_book[Side.BID] = (bid_prices, bid_volumes)
            # self.execute_order_by_etf(bid_prices, ask_prices)

        self.logger.info(f"received order book for instrument {instrument} with sequence number {sequence_number}")

    def execute_order_by_fut(self, bid_prices, ask_prices):
        pass

    def execute_order_by_etf(self, bid_prices, ask_prices):
        pass

    def execute_order_by_tic(self, bid_prices, ask_prices):
        pass

    def calculate_sides_capital(self):
        fut_buy = self._get_total_capital(*self.fut_book[Side.BID])
        fut_sell = self._get_total_capital(*self.fut_book[Side.ASK])

        etf_buy = self._get_total_capital(*self.etf_book[Side.BID])
        etf_sell = self._get_total_capital(*self.etf_book[Side.ASK])
    
        fut_tic_buy = self._get_total_capital(*self.fut_tick[Side.BID])
        fut_tic_sell = self._get_total_capital(*self.fut_tick[Side.ASK])

        etf_tic_buy = self._get_total_capital(*self.etf_tick[Side.BID])
        etf_tic_sell = self._get_total_capital(*self.etf_tick[Side.ASK])

        buy_total_cap = self._get_side_total_capital(fut_buy, etf_buy, fut_tic_buy, etf_tic_buy)
        sell_total_cap = self._get_side_total_capital(fut_sell, etf_sell, fut_tic_sell, etf_tic_sell)

        return (buy_total_cap, sell_total_cap)

    def _get_side_total_capital(self, fut_cap, etf_cap, fut_tic_cap, etf_tic_cap):
        return FUT_CAP_WEIGHT*fut_cap + ETF_CAP_WEIGHT*etf_cap + TIC_CAP_WEIGHT*(fut_tic_cap+etf_tic_cap)
    
    def _get_total_capital(self, prices, vols):
        return sum(map(lambda x: x[0]*x[1], zip(prices, vols)))

    def on_order_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        """Called when one of your orders is filled, partially or fully.

        The price is the price at which the order was (partially) filled,
        which may be better than the order's limit price. The volume is
        the number of lots filled at that price.
        """
        if client_order_id in self.bids:
            self.position += volume
            order_id = next(self.order_ids)
            self.thread_pool.submit(self.send_hedge_order, order_id, Side.ASK, MIN_BID_NEAREST_TICK, volume)
            self.logger.info(f"sending future sell order({order_id}) of price {MIN_BID_NEAREST_TICK} and size {volume}")

        elif client_order_id in self.asks:
            self.position -= volume
            order_id = next(self.order_ids)
            self.thread_pool.submit(self.send_hedge_order, order_id, Side.BID, MAX_ASK_NEAREST_TICK, volume)
            self.logger.info(f"sending future buy order({order_id}) of price {MAX_ASK_NEAREST_TICK} and size {volume}")

        self.logger.info(f"received order({client_order_id} filled of price {price} and volume {volume}")

    def on_order_status_message(self, client_order_id: int, fill_volume: int, remaining_volume: int,
                                fees: int) -> None:
        """Called when the status of one of your orders changes.

        The fill_volume is the number of lots already traded, remaining_volume
        is the number of lots yet to be traded and fees is the total fees for
        this order. Remember that you pay fees for being a market taker, but
        you receive fees for being a market maker, so fees can be negative.

        If an order is cancelled its remaining volume will be zero.
        """
        if remaining_volume == 0:
            if client_order_id == self.bid_id:
                self.bid_id = 0
            elif client_order_id == self.ask_id:
                self.ask_id = 0

            # It could be either a bid or an ask
            self.bids.discard(client_order_id)
            self.asks.discard(client_order_id)

        self.logger.info(f"received order({client_order_id}) status with fill volume {fill_volume} "
                         f"remaining {remaining_volume} and fees {fees}")

    def on_trade_ticks_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                               ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        """Called periodically when there is trading activity on the market.

        The five best ask (i.e. sell) and bid (i.e. buy) prices at which there
        has been trading activity are reported along with the aggregated volume
        traded at each of those price levels.

        If there are less than five prices on a side, then zeros will appear at
        the end of both the prices and volumes arrays.
        """
        if instrument == Instrument.FUTURE:
            # Update local order book
            self.fut_tick[Side.ASK] = (ask_prices, ask_volumes)
            self.fut_tick[Side.BID] = (bid_prices, bid_volumes)
            
        elif instrument == Instrument.ETF:
            # Update local order book
            self.etf_tick[Side.ASK] = (ask_prices, ask_volumes)
            self.etf_tick[Side.BID] = (bid_prices, bid_volumes)

        self.logger.info(f"received trade ticks for instrument {instrument} with sequence number {sequence_number}")
