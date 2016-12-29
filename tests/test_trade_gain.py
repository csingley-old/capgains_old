# coding: utf-8
""" """
# stdlib imports
import unittest
import logging
import sys
from contextlib import contextmanager
from datetime import datetime 
from decimal import Decimal
import os


# 3rd party imports
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ofxtools import ofxalchemy
from ofxtools.ofxalchemy import OFXParser
from ofxtools.ofxalchemy.models import (
    INVACCTFROM,
    SECINFO,
    STOCKINFO,
    INVTRAN,
    BUYSTOCK,
    SELLSTOCK,
    RETOFCAP,
)


# local imports
from capgains.models import (
    Base,
    Lot,
    Gain,
)


### LOGGING SETUP
#logger = logging.getLogger()
#logger.level = logging.INFO


### DB SETUP
engine = create_engine('sqlite:///test.db', echo=False)
Session = sessionmaker(bind=engine)

@contextmanager
def session_scope():
    """
    Provide a transactional scope around a series of database operations.
    """
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


### TEST CASES
class AlchemyTestCase(unittest.TestCase):
    def setUp(self):
        ofxalchemy.models.Base.metadata.create_all(engine)
        Base.metadata.create_all(engine)

    def tearDown(self):
        pass
        try:
            os.unlink('test.db')
        except OSError:  # file not created by test -- probably an error
            pass


#class LoggingTestCase(AlchemyTestCase):
    #def setUp(self):
        #super(LoggingTestCase, self).setUp()
        #stream_handler = logging.StreamHandler(sys.stdout)
        #logger.addHandler(stream_handler)

    #def tearDown(self):
        #super(LoggingTestCase, self).tearDown()
        #stream_handler = logging.StreamHandler(sys.stdout)
        #logger.removeHandler(stream_handler)


class TradeGainTestCase(AlchemyTestCase):
    def testTrade(self):
        with session_scope() as DBSession:
            acctfrom = INVACCTFROM(brokerid='2222', acctid='271828')
            DBSession.add(acctfrom)

            security = STOCKINFO(
                uniqueid='123456789', uniqueidtype='CUSIP',
                secname='Acme Development, Inc.', ticker='ACME', fiid='1024',
            )
            DBSession.add(security)

            trade1 = BUYSTOCK(
                acctfrom=acctfrom,
                fitid='a', dttrade=datetime(2005, 10, 3), secinfo=security,
                buytype='BUY', units=Decimal('300'), unitprice=Decimal('10.00'),
                commission=Decimal('9.99'), total=Decimal('-3009.99'),
                subacctsec='CASH', subacctfund='CASH'
            )
            DBSession.add(trade1)

            trade2 = SELLSTOCK(
                acctfrom=acctfrom,
                fitid='b', dttrade=datetime(2005, 12, 1), secinfo=security,
                selltype='SELL', units=Decimal('-200'), unitprice=Decimal('12'),
                commission=Decimal('9.99'), total=Decimal('2390.01'),
                subacctsec='CASH', subacctfund='CASH',
            )
            DBSession.add(trade2)

            # Calculate Lots and Gains on the trades
            Lot.trade(DBSession, trade1)
            Lot.trade(DBSession, trade2)

            self._invstmtrs2_test(DBSession)

            # No wash sales, so results should be the same after calculation
            Gain.doWashSales(DBSession)

            self._invstmtrs2_test(DBSession)

    def _invstmtrs2_test(self, DBSession):
        trade1, trade2 = DBSession.query(INVTRAN).order_by(INVTRAN.dttrade).all()
        # When Lots/Gains are calculated, the initial Lot of 300sh gets
        # split in 2.
        lots = DBSession.query(Lot).all()
        self.assertEqual(len(lots), 2)
        lot1, lot2 = lots

        # Lot 1: 200sh closed, cost 200/300 * -3,009.99 = $2,006.66
        self.assertEqual(lot1.units, Decimal('200'))
        self.assertEqual(lot1.cost, Decimal('2006.66'))
        self.assertEqual(lot1.opener, trade1)
        self.assertEqual(lot1.dtclose, datetime(2005, 12, 1))
        #self.assertEqual(lot1.washed, False)

        # Lot 2: 100sh open, cost 100/300 * -3,009.99 = $1,003.33
        self.assertEqual(lot2.units, Decimal('100'))
        self.assertEqual(lot2.cost, Decimal('1003.33'))
        self.assertEqual(lot2.opener, trade1)
        self.assertEqual(lot2.dtclose, None)
        #self.assertEqual(lot2.washed, False)

        # Gain on sale: proceeds of 200 @ $12.00 - $9.99 = $2,390.01
        #               less cost of $2,006.66 = gain of $383.35 (STCG) 
        gain = DBSession.query(Gain).one()
        self.assertEqual(gain.proceeds, Decimal('2390.01'))
        self.assertEqual(gain.washloss, Decimal('0')) 
        self.assertEqual(gain.lot, lot1)
        self.assertEqual(gain.transaction, trade2)
        self.assertEqual(gain.cost, Decimal('2006.66'))
        self.assertEqual(gain.value, Decimal('383.35'))
        self.assertFalse(gain.isLongTerm)

        # Test the relationship from Lot to Gain
        self.assertEqual(len(lot1.gains), 1)
        self.assertEqual(lot1.gains[0], gain)

        self.assertEqual(len(lot2.gains), 0)


    def testWashSale1(self):
        """
        Wash sale where replacement units don't consume all the loss units
        """
        with session_scope() as DBSession:
            acctfrom = INVACCTFROM(brokerid='2222', acctid='271828')
            DBSession.add(acctfrom)

            # Create some OFX import data
            security = STOCKINFO(
                uniqueid='123456789', uniqueidtype='CUSIP',
                secname='Acme Development, Inc.', ticker='ACME', fiid='1024',
            )
            DBSession.add(security)

            trade1 = BUYSTOCK(
                acctfrom=acctfrom,
                fitid='a', dttrade=datetime(2005, 10, 3), secinfo=security,
                buytype='BUY', units=Decimal('300'), unitprice=Decimal('10.00'),
                commission=Decimal('9.99'), total=Decimal('-3009.99'),
                subacctsec='CASH', subacctfund='CASH'
            )
            DBSession.add(trade1)

            trade2 = BUYSTOCK(
                acctfrom=acctfrom,
                fitid='b', dttrade=datetime(2005, 11, 1), secinfo=security,
                buytype='BUY', units=Decimal('300'), unitprice=Decimal('5.00'),
                commission=Decimal('9.99'), total=Decimal('-1509.99'),
                subacctsec='CASH', subacctfund='CASH',
            )
            DBSession.add(trade2)

            trade3 = SELLSTOCK(
                acctfrom=acctfrom,
                fitid='c', dttrade=datetime(2005, 12, 1), secinfo=security,
                selltype='SELL', units=Decimal('-400'), 
                unitprice=Decimal('8.00'), commission=Decimal('9.99'), 
                total=Decimal('3190.01'), subacctsec='CASH', subacctfund='CASH',
            )
            DBSession.add(trade3)

            # Calculate Lots and Gains on the trades
            Lot.trade(DBSession, trade1)
            Lot.trade(DBSession, trade2)
            Lot.trade(DBSession, trade3)

            # When Lots/Gains are calculated, the first Lot of 300sh gets
            # closed unmodified, and the second lot gets split in 2.
            lots = DBSession.query(Lot).all()
            self.assertEqual(len(lots), 3)
            lot1, lot2, lot3 = lots

            # Lot 1: 300sh closed, cost -3,009.99
            self.assertEqual(lot1.units, Decimal('300'))
            self.assertEqual(lot1.cost, Decimal('3009.99'))
            self.assertEqual(lot1.dtopen, trade1.dttrade)
            self.assertEqual(lot1.opener, trade1)
            #self.assertEqual(lot1.dtclose, datetime(2005, 12, 1))
            self.assertEqual(lot1.dtclose, trade3.dttrade)
            self.assertEqual(lot1.closer, trade3)
            #self.assertEqual(lot1.washed, False)

            # Lot 2: 100sh closed, cost 100/300 * -1,509.99 = $503.33
            self.assertEqual(lot2.units, Decimal('100'))
            self.assertEqual(lot2.cost, Decimal('503.33'))
            self.assertEqual(lot2.dtopen, trade2.dttrade)
            self.assertEqual(lot2.opener, trade2)
            self.assertEqual(lot2.dtclose, datetime(2005, 12, 1))
            self.assertEqual(lot2.closer, trade3)
            #self.assertEqual(lot2.washed, False)

            # Lot 3: 200sh open, cost 200/300 * -1,509.99 = $1,006.66
            self.assertEqual(lot3.units, Decimal('200'))
            self.assertEqual(lot3.cost, Decimal('1006.66'))
            self.assertEqual(lot3.dtopen, trade2.dttrade)
            self.assertEqual(lot3.opener, trade2)
            self.assertEqual(lot3.dtclose, None)
            self.assertEqual(lot3.closer, None)
            #self.assertEqual(lot3.washed, False)

            gains = DBSession.query(Gain).filter(
                Gain.invtran_id == INVTRAN.id,
            ).order_by(INVTRAN.dttrade).all()
            self.assertEqual(len(gains), 2)
            gain1, gain2 = gains

            # Gain 1: proceeds of 300/400 * $3,190.01 = $2,392.5075
            #         less cost of $3,009.99 = loss of -$617.4825 (STCG) 
            self.assertEqual(gain1.lot, lot1)
            self.assertEqual(gain1.proceeds, Decimal('2392.5075'))
            self.assertEqual(gain1.transaction, trade3)
            self.assertEqual(gain1.cost, Decimal('3009.99'))
            self.assertEqual(gain1.value, Decimal('-617.4825'))
            self.assertEqual(gain1.washloss, Decimal('0')) 
            self.assertFalse(gain1.isLongTerm)

            # Gain 2: proceeds of 100/400 * $3,190.01 = $797.5025
            #         less cost of $503.33 = gain of $294.1725 (STCG) 
            self.assertEqual(gain2.lot, lot2)
            self.assertEqual(gain2.proceeds, Decimal('797.5025'))
            self.assertEqual(gain2.transaction, trade3)
            self.assertEqual(gain2.cost, Decimal('503.33'))
            self.assertEqual(gain2.value, Decimal('294.1725'))
            self.assertEqual(gain2.washloss, Decimal('0')) 
            self.assertFalse(gain2.isLongTerm)

            # Test the relationship from Lot to Gain
            self.assertEqual(len(lot1.gains), 1)
            self.assertEqual(lot1.gains[0], gain1)

            self.assertEqual(len(lot2.gains), 1)
            self.assertEqual(lot2.gains[0], gain2)

            # Account for wash sales
            Gain.doWashSales(DBSession)

            # When wash sales are accounted for, all of trade2 units are
            # replacement shares for trade3 units.  As such, all of gain1
            # is a wash sale; its loss is disallowed and rolled into the 
            # cost basis of Lots created by trade2 (i.e. lot2 and lot 3).
            self.assertEqual(DBSession.query(Lot).count(), 3)

            # Lot 1 should be unaffected by the wash sale calculations
            self.assertEqual(lot1.units, Decimal('300'))
            self.assertEqual(lot1.cost, Decimal('3009.99'))
            self.assertEqual(lot1.opener, trade1)
            self.assertEqual(lot1.dtclose, datetime(2005, 12, 1))

            # Lot 2 gets extra cost basis of 100/300 * $617.4825 = -$205.8275
            self.assertEqual(lot2.units, Decimal('100'))
            self.assertEqual(lot2.cost, Decimal('503.33'))
            self.assertEqual(lot2.washcost, Decimal('205.8275'))
            self.assertEqual(lot2.opener, trade2)
            self.assertEqual(lot2.dtclose, datetime(2005, 12, 1))

            # Lot 3 gets extra cost basis of 200/300 * $617.4825 = -$411.6550
            self.assertEqual(lot3.units, Decimal('200'))
            self.assertEqual(lot3.cost, Decimal('1006.66'))
            self.assertEqual(lot3.washcost, Decimal('411.6550'))
            self.assertEqual(lot3.opener, trade2)
            self.assertEqual(lot3.dtclose, None)
            #self.assertEqual(lot3.washed, True)

            # No new Gains should have been created
            self.assertEqual(DBSession.query(Gain).count(), 2)

            # Gain 1: proceeds of 300/400 * $3,190.01 = $2,392.5075
            #         less cost of $3,009.99 = loss of -$617.4825 (STCG) 
            #         Loss of -$617.4825 disallowed
            self.assertEqual(gain1.lot, lot1)
            self.assertEqual(gain1.transaction, trade3)
            self.assertEqual(gain1.proceeds, Decimal('2392.5075'))
            self.assertEqual(gain1.units, Decimal('300'))
            self.assertEqual(gain1.cost, Decimal('3009.99'))
            self.assertEqual(gain1.value, Decimal('-617.4825'))
            self.assertEqual(gain1.washcost, Decimal('0'))
            self.assertEqual(gain1.taxcost, gain1.cost)
            self.assertEqual(gain1.washloss, Decimal('-617.4825')) 
            self.assertEqual(gain1.taxvalue, gain1.value)
            self.assertFalse(gain1.isLongTerm)

            # Gain 2: proceeds of 100/400 * $3,190.01 = $797.5025
            #         less cost of $503.33 = gain of $294.1725 (STCG) 
            #         Disallowed loss of 100/300 * $617.4825 = -$205.8275
            #         rolled into basis
            self.assertEqual(gain2.lot, lot2)
            self.assertEqual(gain2.transaction, trade3)
            self.assertEqual(gain2.proceeds, Decimal('797.5025'))
            self.assertEqual(gain2.units, Decimal('100'))
            self.assertEqual(gain2.cost, Decimal('503.33'))
            self.assertEqual(gain2.value, Decimal('294.1725'))
            self.assertEqual(gain2.washcost, Decimal('205.8275'))
            self.assertEqual(gain2.taxcost, gain2.cost + gain2.washcost)
            self.assertEqual(gain2.washloss, Decimal('0')) 
            self.assertEqual(gain2.taxvalue, gain2.proceeds - gain2.taxcost)
            self.assertFalse(gain2.isLongTerm)

    def testWashSale2(self):
        """
        Wash sale where there are more replacement units than units
        where gain is disallowed
        """
        with session_scope() as DBSession:
            acctfrom = INVACCTFROM(brokerid='2222', acctid='271828')
            DBSession.add(acctfrom)

            # Create some OFX import data
            security = STOCKINFO(
                uniqueid='123456789', uniqueidtype='CUSIP',
                secname='Acme Development, Inc.', ticker='ACME', fiid='1024',
            )
            DBSession.add(security)

            trade1 = BUYSTOCK(
                acctfrom=acctfrom,
                fitid='a', dttrade=datetime(2005, 10, 3), secinfo=security,
                buytype='BUY', units=Decimal('200'), unitprice=Decimal('10.00'),
                commission=Decimal('9.99'), total=Decimal('-2009.99'),
                subacctsec='CASH', subacctfund='CASH'
            )
            DBSession.add(trade1)

            trade2 = BUYSTOCK(
                acctfrom=acctfrom,
                fitid='b', dttrade=datetime(2005, 11, 1), secinfo=security,
                buytype='BUY', units=Decimal('500'), unitprice=Decimal('5.00'),
                commission=Decimal('9.99'), total=Decimal('-2509.99'),
                subacctsec='CASH', subacctfund='CASH',
            )
            DBSession.add(trade2)

            trade3 = SELLSTOCK(
                acctfrom=acctfrom,
                fitid='c', dttrade=datetime(2005, 12, 1), secinfo=security,
                selltype='SELL', units=Decimal('-500'),
                unitprice=Decimal('8.00'), commission=Decimal('9.99'),
                total=Decimal('3990.01'), subacctsec='CASH', subacctfund='CASH',
            )
            DBSession.add(trade3)

            # Calculate Lots and Gains on the trades
            Lot.trade(DBSession, trade1)
            Lot.trade(DBSession, trade2)
            Lot.trade(DBSession, trade3)

            # When Lots/Gains are calculated, the first Lot of 300sh gets
            # closed unmodified, and the second lot gets split in 2.
            lots = DBSession.query(Lot).filter(
                Lot.opener_id== INVTRAN.id,).order_by(INVTRAN.dttrade)
            self.assertEqual(lots.count(), 3)
            lot1, lot2, lot3 = lots

            # Lot 1: 200sh closed, cost 2,009.99
            self.assertEqual(lot1.units, Decimal('200'))
            self.assertEqual(lot1.cost, Decimal('2009.99'))
            self.assertEqual(lot1.dtopen, trade1.dttrade)
            self.assertEqual(lot1.opener, trade1)
            self.assertEqual(lot1.dtclose, trade3.dttrade)
            self.assertEqual(lot1.closer, trade3)

            # Lot 2: 300sh closed, cost $1,505.99
            self.assertEqual(lot2.units, Decimal('300'))
            self.assertEqual(lot2.cost, Decimal('1505.994'))
            self.assertEqual(lot2.dtopen, trade2.dttrade)
            self.assertEqual(lot2.opener, trade2)
            self.assertEqual(lot2.dtclose, trade3.dttrade)
            self.assertEqual(lot2.closer, trade3)

            # Lot 3: 200sh open, cost -$1,004.00
            self.assertEqual(lot3.units, Decimal('200'))
            self.assertEqual(lot3.cost, Decimal('1003.996'))
            self.assertEqual(lot3.dtopen, trade2.dttrade)
            self.assertEqual(lot3.opener, trade2)
            self.assertEqual(lot3.dtclose, None)
            self.assertEqual(lot3.closer, None)

            gains = DBSession.query(Gain)
            self.assertEqual(gains.count(), 2)
            gain1, gain2 = gains

            # Gain 1: proceeds $1,596.00 - cost $2,009.99 = loss -$413.99 (STCG) 
            self.assertEqual(gain1.proceeds, Decimal('1596.004'))
            self.assertEqual(gain1.lot, lot1)
            self.assertEqual(gain1.transaction, trade3)
            self.assertEqual(gain1.units, Decimal('200'))
            self.assertEqual(gain1.cost, Decimal('2009.99'))
            self.assertEqual(gain1.proceeds, Decimal('1596.004'))
            self.assertEqual(gain1.value, Decimal('-413.986'))
            self.assertEqual(gain1.washloss, Decimal('0')) 
            self.assertFalse(gain1.isLongTerm)

            # Gain 2: proceeds $1,596.00 - cost $2,009.99 = loss -$413.99 (STCG) 
            self.assertEqual(gain2.proceeds, Decimal('2394.006'))
            self.assertEqual(gain2.lot, lot2)
            self.assertEqual(gain2.transaction, trade3)
            self.assertEqual(gain2.units, Decimal('300'))
            self.assertEqual(gain2.cost, Decimal('1505.994'))
            self.assertEqual(gain2.proceeds, Decimal('2394.006'))
            self.assertEqual(gain2.value, Decimal('888.012'))
            self.assertEqual(gain2.washloss, Decimal('0')) 
            self.assertFalse(gain2.isLongTerm)

            # Test the relationship from Lot to Gain
            self.assertEqual(len(lot1.gains), 1)
            self.assertIs(lot1.gains[0], gain1)

            self.assertEqual(len(lot2.gains), 1)
            self.assertIs(lot2.gains[0], gain2)

            # Account for wash sales
            Gain.doWashSales(DBSession)

            # When wash sales are accounted for, 200 of lot2's 300 units are
            # replacement shares for gain1 units.  As such, lot2
            # gets split into a Lot of 200 replacement units, with the
            # disallowed loss on gain1 rolled in, and 100 unwashed shares.
            # When lot2 gets split, its corresponding gain2 also gets
            # split accordingly.
            lots = DBSession.query(Lot).filter(
                Lot.opener_id == INVTRAN.id,
            ).order_by(INVTRAN.dttrade)
            self.assertEqual(lots.count(), 4)
            lot1, lot2, lot3, lot4 = lots

            # Lot 1 should be unaffected by the wash sale calculations
            self.assertEqual(lot1.units, Decimal('200'))
            self.assertEqual(lot1.cost, Decimal('2009.99'))
            self.assertEqual(lot1.dtopen, trade1.dttrade)
            self.assertEqual(lot1.opener, trade1)
            self.assertEqual(lot1.dtclose, trade3.dttrade)
            self.assertEqual(lot1.closer, trade3)

            # Lot 2 gets split - 200 units are replacement shares for lot1.
            # lot1 was closed in gain1; all of gain1's loss (i.e. -413.986)
            # is disallowed and rolled into lot2's cost basis.
            self.assertEqual(lot2.units, Decimal('200'))
            self.assertEqual(lot2.cost, Decimal('1003.996'))
            self.assertEqual(lot2.washcost,  Decimal('413.986'))
            self.assertEqual(lot2.dtopen, trade2.dttrade)
            self.assertEqual(lot2.opener, trade2)
            self.assertEqual(lot2.dtclose, trade3.dttrade)
            self.assertEqual(lot2.closer, trade3)

            # Lot 3 should be unaffected by the wash sale calculations
            self.assertEqual(lot1.units, Decimal('200'))
            self.assertEqual(lot3.units, Decimal('200'))
            self.assertEqual(lot3.cost, Decimal('1003.996'))
            self.assertEqual(lot3.dtopen, trade2.dttrade)
            self.assertEqual(lot3.opener, trade2)
            self.assertEqual(lot3.dtclose, None)
            self.assertEqual(lot3.closer, None)

            # Lot 4 is the extra units from Lot 2
            # Cost basis of 100/300 * $1,505.99 = $501.998
            self.assertEqual(lot4.units, Decimal('100'))
            self.assertEqual(lot4.cost, Decimal('501.998'))
            self.assertEqual(lot4.dtopen, trade2.dttrade)
            self.assertEqual(lot4.opener, trade2)
            self.assertEqual(lot4.dtclose, trade3.dttrade)
            self.assertEqual(lot4.closer, trade3)

            gains = DBSession.query(Gain)
            self.assertEqual(gains.count(), 3)
            gain1, gain2, gain3 = gains

            # gain1: value of -$413.986 is disallowed
            self.assertEqual(gain1.lot, lot1)
            self.assertEqual(gain1.transaction, trade3)
            self.assertEqual(gain1.proceeds, Decimal('1596.004'))
            self.assertEqual(gain1.units, Decimal('200'))
            self.assertEqual(gain1.cost, Decimal('2009.99'))
            self.assertEqual(gain1.proceeds, Decimal('1596.004'))
            self.assertEqual(gain1.value, Decimal('-413.986'))
            self.assertEqual(gain1.washloss, Decimal('-413.986')) 
            self.assertFalse(gain1.isLongTerm)

            # gain2 has been split; it's now the 200 replacement shares
            self.assertEqual(gain2.lot, lot2)
            self.assertEqual(gain2.transaction, trade3)
            self.assertEqual(gain2.units, Decimal('200'))
            self.assertEqual(gain2.cost, Decimal('1003.996'))
            self.assertEqual(gain2.proceeds, Decimal('1596.004'))
            self.assertEqual(gain2.value, Decimal('592.008'))
            self.assertEqual(gain2.washloss, Decimal('0')) 
            self.assertFalse(gain2.isLongTerm)

            # gain3 is the 100sh split from gain2 that aren't replacement shares
            self.assertEqual(gain3.lot, lot4)
            self.assertEqual(gain3.transaction, trade3)
            self.assertEqual(gain3.units, Decimal('100'))
            self.assertEqual(gain3.cost, Decimal('501.998'))
            self.assertEqual(gain3.proceeds, Decimal('798.002'))
            self.assertEqual(gain3.value, Decimal('296.004'))
            self.assertEqual(gain3.washloss, Decimal('0')) 
            self.assertFalse(gain3.isLongTerm)


class ReturnOfCapitalTestCase(AlchemyTestCase):
    def testReturnOfCapital(self):
        with session_scope() as DBSession:
            acctfrom = INVACCTFROM(brokerid='2222', acctid='271828')
            DBSession.add(acctfrom)

            # Create some OFX import data
            security = STOCKINFO(
                uniqueid='123456789', uniqueidtype='CUSIP',
                secname='Acme Development, Inc.', ticker='ACME', fiid='1024',
            )
            DBSession.add(security)

            trade1 = BUYSTOCK(
                acctfrom=acctfrom,
                fitid='a', dttrade=datetime(2005, 10, 3), secinfo=security,
                buytype='BUY', units=Decimal('300'), unitprice=Decimal('10.00'),
                commission=Decimal('9.99'), total=Decimal('-3009.99'),
                subacctsec='CASH', subacctfund='CASH'
            )
            DBSession.add(trade1)

            # Create the Lot
            Lot.trade(DBSession, trade1)

            # 1st return of capital - $3,000
            # Remaining basis of $9.99; no gain
            retofcap1 = RETOFCAP(
                acctfrom=acctfrom,
                fitid='b', dttrade=datetime(2005, 10, 4), secinfo=security,
                total=Decimal('3000'), subacctsec='CASH', subacctfund='CASH'
            )
            DBSession.add(retofcap1)
            Lot.returnOfCapital(DBSession, retofcap1)

            # 2nd return of capital - $1,000
            # Basis reduced to 0; gain of $990.01
            retofcap2 = RETOFCAP(
                acctfrom=acctfrom,
                fitid='c', dttrade=datetime(2005, 10, 5), secinfo=security,
                total=Decimal('1000'), subacctsec='CASH', subacctfund='CASH'
            )
            DBSession.add(retofcap2)
            Lot.returnOfCapital(DBSession, retofcap2)

            # Before the returns of capital, there should be one Lot
            # with cost=$3,009.99 and no Gain
            lots = Lot.asOf(DBSession, datetime(2005, 10, 3)).all()
            self.assertEqual(len(lots), 1)
            lot = lots[0]
            
            self.assertEqual(lot.units, trade1.units)
            self.assertEqual(lot.cost, -trade1.total)
            self.assertEqual(lot.dtopen, trade1.dttrade)
            self.assertEqual(lot.opener, trade1)
            self.assertEqual(lot.dtclose, None)
            self.assertEqual(lot.closer, None)
            self.assertEqual(lot.starter, trade1)
            self.assertEqual(lot.dtstart, trade1.dttrade)
            self.assertEqual(lot.ender, retofcap1)
            self.assertEqual(lot.dtend, retofcap1.dttrade)
            self.assertEqual(lot.predecessor, None)
            self.assertEqual(lot.washcost, Decimal('0'))
            self.assertEqual(len(lot.gains), 0)

            # After the 1st return of capital, there should be one Lot
            # with cost=$9.99 and no Gain
            lots = Lot.asOf(DBSession, datetime(2005, 10, 4)).all()
            self.assertEqual(len(lots), 1)
            lot = lots[0]
            
            self.assertEqual(lot.units, Decimal('300'))
            #self.assertEqual(lot.cost, Decimal('9.99'))
            self.assertEqual(lot.cost, -trade1.total - retofcap1.total)
            self.assertEqual(lot.dtopen, trade1.dttrade)
            self.assertEqual(lot.opener, trade1)
            self.assertEqual(lot.dtclose, None)
            self.assertEqual(lot.closer, None)
            self.assertEqual(lot.starter, retofcap1)
            self.assertEqual(lot.dtstart, retofcap1.dttrade)
            self.assertEqual(lot.ender, retofcap2)
            self.assertEqual(lot.dtend, retofcap2.dttrade)
            #self.assertEqual(lot.predecessor, None)
            self.assertEqual(lot.washcost, Decimal('0'))
            self.assertEqual(len(lot.gains), 0)

            # After the 2nd return of capital, there should be one Lot
            # with zero cost and a Gain of $990.01
            lots = Lot.asOf(DBSession, datetime(2005, 10, 5)).all()
            self.assertEqual(len(lots), 1)
            lot = lots[0]
            
            self.assertEqual(lot.units, Decimal('300'))
            self.assertEqual(lot.cost, Decimal('0'))
            self.assertEqual(lot.dtopen, trade1.dttrade)
            self.assertEqual(lot.opener, trade1)
            self.assertEqual(lot.dtclose, None)
            self.assertEqual(lot.closer, None)
            self.assertEqual(lot.starter, retofcap2)
            self.assertEqual(lot.dtstart, retofcap2.dttrade)
            self.assertEqual(lot.dtend, None)
            self.assertEqual(lot.ender, None)
            #self.assertEqual(lot.predecessor, None)
            self.assertEqual(lot.washcost, Decimal('0'))

            self.assertEqual(len(lot.gains), 1)
            gain = lot.gains[0]
            self.assertAlmostEqual(gain.proceeds, Decimal('990.01'))
            self.assertEqual(gain.washloss, Decimal('0')) 
            self.assertEqual(gain.lot, lot)
            self.assertEqual(gain.transaction, retofcap2)
            self.assertEqual(gain.cost, Decimal('0'))
            self.assertAlmostEqual(gain.value, Decimal('990.01'))
            self.assertFalse(gain.isLongTerm)


if __name__=='__main__':
    unittest.main()
