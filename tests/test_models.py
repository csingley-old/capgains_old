# coding: utf-8

# stdlib imports
import unittest
from contextlib import contextmanager
from decimal import Decimal
from datetime import datetime
import os
from operator import attrgetter


# 3rd party imports
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ofxtools import ofxalchemy
from ofxtools.ofxalchemy.models import (
    INVACCTFROM,
    STOCKINFO,
    INVTRAN,
    BUYSTOCK,
    SELLSTOCK,
)


# local imports
from capgains.models import (
    Base,
    Lot,
    OfxLog,
)


### DB SETUP
dbfile = 'test.db'
engine = create_engine('sqlite:///%s' % dbfile, echo=False)
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

class LotTestCase(unittest.TestCase):
    def setUp(self):
        ofxalchemy.models.Base.metadata.create_all(engine)
        Base.metadata.create_all(engine)

        self.acctfrom = INVACCTFROM(brokerid='2222', acctid='271828')

        # Buy BKRA and leave it open
        self.secinfo1 = STOCKINFO(uniqueidtype='CUSIP', uniqueid='084670108',
                                ticker='BRK-A',secname='Berkshire Hathaway')
        self.invtran1 = BUYSTOCK(
            acctfrom=self.acctfrom,
            fitid='a', dttrade=datetime(2016, 1, 4, 9, 30),
            buytype='BUY', secinfo=self.secinfo1,
            units=Decimal('1'), unitprice=Decimal('193300'),
            commission=Decimal('9.99'), total=Decimal('-193309.99'),
            subacctsec='CASH', subacctfund='CASH'
        )
        self.brkaLot = Lot(security=self.secinfo1, account=self.acctfrom,
                           dtopen=self.invtran1.dttrade, opener=self.invtran1,
                           dtstart=self.invtran1.dttrade, starter=self.invtran1,
                           units=self.invtran1.units, cost=-self.invtran1.total,
                          )
        
        # Buy IBKR then close it
        self.secinfo2 = STOCKINFO(uniqueidtype='CUSIP', uniqueid='45841N107',
                                ticker='IBKR',secname='Interactive Brokers')
        self.invtran2 = BUYSTOCK(
            acctfrom=self.acctfrom,
            fitid='b', dttrade=datetime(2016, 1, 4, 12, 45),
            buytype='BUY', secinfo=self.secinfo2,
            units=Decimal('100'), unitprice=Decimal('42.24'),
            commission=Decimal('9.99'), total=Decimal('-4233.99'),
            subacctsec='CASH', subacctfund='CASH'
        )
        self.invtran3 = SELLSTOCK(
            acctfrom=self.acctfrom,
            fitid='c', dttrade=datetime(2016, 1, 10, 10, 15),
            selltype='SELL', secinfo=self.secinfo2,
            units=Decimal('-100'), unitprice=Decimal('43.35'),
            commission=Decimal('9.99'), total=Decimal('4325.01'),
            subacctsec='CASH', subacctfund='CASH'
        )
        self.ibkrLot = Lot(security=self.secinfo2, account=self.acctfrom,
                           units=self.invtran2.units, cost=-self.invtran2.total,
                           dtopen=self.invtran2.dttrade, opener=self.invtran2,
                           dtclose=self.invtran3.dttrade, closer=self.invtran3,
                           dtstart=self.invtran2.dttrade, starter=self.invtran2,
                           dtend=self.invtran3.dttrade, ender=self.invtran3,
                          )

        # Sell KMI short and leave it open
        self.secinfo3 = STOCKINFO(uniqueidtype='CUSIP', uniqueid='49456B101',
                                  ticker='KMI', secname='Kinder Morgan')
        self.invtran4 = SELLSTOCK(
            acctfrom=self.acctfrom,
            fitid='d', dttrade=datetime(2016, 1, 4, 9, 30),
            selltype='SELL', secinfo=self.secinfo3,
            units=Decimal('-100'), unitprice=Decimal('18.25'),
            commission=Decimal('9.99'), total=Decimal('1815.01'),
            subacctsec='CASH', subacctfund='CASH'
        )
        self.kmiLot = Lot(security=self.secinfo3, account=self.acctfrom,
                          dtopen=self.invtran4.dttrade, opener=self.invtran4,
                          dtstart=self.invtran4.dttrade, starter=self.invtran4,
                          units=self.invtran4.units, cost=-self.invtran4.total,
                         )

    def tearDown(self):
        pass
        try:
            os.unlink(dbfile)
        except OSError:  # file not created by test -- probably an error
            pass

    def testLotUnitCost(self):
        with session_scope() as DBSession:
            DBSession.add(self.ibkrLot)
            self.assertEqual(self.ibkrLot.unitCost, Decimal('42.3399'))

    def testLotAsOf(self):
        with session_scope() as DBSession:
            # BRK buy
            DBSession.add(self.invtran1)
            DBSession.add(self.brkaLot)

            # IBKR buy
            DBSession.add(self.invtran2)
            DBSession.add(self.ibkrLot)

            # Mock IBKR Lot being closed by self.invtran3
            DBSession.add(self.invtran3)
            self.ibkrLot.dtclose = self.invtran3.dttrade
            self.ibkrLot.dtend = self.invtran3.dttrade
            DBSession.add(self.ibkrLot)

            # Create KMI short
            DBSession.add(self.invtran4)
            DBSession.add(self.kmiLot)

            # Before the first INVTRAN there should be no Lots
            self.assertEqual(Lot.asOf(DBSession, datetime(2015,1,1)).all(), [])

            # At moment of the BRKA/KMI trades, those should be the only Lots.
            lots = Lot.asOf(DBSession, self.invtran1.dttrade).all()
            self.assertEqual(len(lots), 2)
            self.assertIn(self.brkaLot, lots)
            self.assertIn(self.kmiLot, lots)

            # At the moment of the IBKR purchase, there should be two long Lots 
            # (BRKA & IBKR).  They should be returned in chronological order
            # by purchase date.
            lots = Lot.asOf(DBSession, self.invtran2.dttrade).all()
            self.assertEqual(len(lots), 3)
            self.assertIn(self.brkaLot, lots)
            self.assertIn(self.kmiLot, lots)
            self.assertIn(self.ibkrLot, lots)

            # At the moment recorded for the closing of the IBKR lot, only the
            # BKRA Lot should remain as an open long. KMI is an open short
            lots = Lot.asOf(DBSession, self.ibkrLot.dtclose).all()
            self.assertEqual(len(lots), 2)
            #self.assertIs(lots[0], self.brkaLot)
            #self.assertIs(lots[1], self.kmiLot)
            self.assertIn(self.brkaLot, lots)
            self.assertIn(self.kmiLot, lots)

    def testLotLongsAsOf(self):
        with session_scope() as DBSession:
            # Mock IBKR Lot being closed by self.invtran3
            DBSession.add(self.invtran3)
            self.ibkrLot.dtclose = self.invtran3.dttrade
            DBSession.add(self.ibkrLot)

            # Before the first INVTRAN there should be no Lots
            self.assertEqual(Lot.longsAsOf(DBSession, datetime(2015,1,1)).all(),
                             [])
            DBSession.add(self.invtran4)
            DBSession.add(self.kmiLot)

            # At moment of the BRKA purchase, that should be the only long Lot
            # (KMI transaction is simultaneous, but it's a short sale)
            DBSession.add(self.invtran1)
            DBSession.add(self.brkaLot)
            lots = Lot.longsAsOf(DBSession, self.invtran1.dttrade).all()
            self.assertEqual(len(lots), 1)
            self.assertIs(lots[0], self.brkaLot)

            # At the moment of the IBKR purchase, there should be two Lots 
            # (BRKA & IBKR).  They should be returned in chronological order
            # by purchase date.
            DBSession.add(self.invtran2)
            DBSession.add(self.ibkrLot)
            lots = Lot.longsAsOf(DBSession, self.invtran2.dttrade).all()
            self.assertEqual(len(lots), 2)
            self.assertIs(lots[0], self.brkaLot)
            self.assertIs(lots[1], self.ibkrLot)

            # At the moment recorded for the closing of the IBKR lot, only the
            # BKRA Lot should remain as an open long.
            lots = Lot.longsAsOf(DBSession, self.ibkrLot.dtclose).all()
            self.assertEqual(len(lots), 1)
            self.assertIs(lots[0], self.brkaLot)

    def testLotCsv(self):
        """ Lot should be unchanged after round trip dump/load CSV file """
        csvfile = 'test.csv'
        oldlots = [self.brkaLot, self.ibkrLot, self.kmiLot]
        oldlots.sort(key=attrgetter('dtopen'))
        with session_scope() as DBSession:
            DBSession.add_all(oldlots)
            Lot.dumpCsv(DBSession, csvfile, dtasof=self.ibkrLot.dtopen)

        # Get a new Session to make sure objects are fetched from DB
        with session_scope() as DBSession:
            DBSession.add_all(oldlots)
            lots = Lot.loadCsv(DBSession, csvfile)
            DBSession.add_all(lots)
            lots.sort(key=attrgetter('dtopen'))
            self.assertEqual(len(lots), 3)
            for (lot, oldlot) in zip(lots, oldlots):
                # CSV dumps don't preserve opening INVTRAN ('opener')
                for attr in ('account', 'security', 'dtopen', 'dtstart',
                             'units', 'cost', 'washcost'):
                    self.assertEqual(getattr(lot, attr), getattr(oldlot, attr))

        try:
            os.unlink(csvfile)
        except OSError:  # file not created by test -- probably an error
            pass

class OfxLogTestCase(unittest.TestCase):
    def setUp(self):
        ofxalchemy.models.Base.metadata.create_all(engine)
        Base.metadata.create_all(engine)

        self.acctfrom = INVACCTFROM(brokerid='2222', acctid='271828')

        self.secinfo1 = STOCKINFO(uniqueidtype='CUSIP', uniqueid='084670108',
                                ticker='BRK-A',secname='Berkshire Hathaway')

        self.invtran1 = BUYSTOCK(
            acctfrom=self.acctfrom,
            fitid='a', dttrade=datetime(2016, 1, 4, 9, 30),
            buytype='BUY', secinfo=self.secinfo1,
            units=Decimal('1'), unitprice=Decimal('193300'),
            commission=Decimal('9.99'), total=Decimal('-193309.99'),
            subacctsec='CASH', subacctfund='CASH'
        )

        self.secinfo2 = STOCKINFO(uniqueidtype='CUSIP', uniqueid='45841N107',
                                ticker='IBKR',secname='Interactive Brokers')

        self.invtran2 = BUYSTOCK(
            acctfrom=self.acctfrom,
            fitid='b', dttrade=datetime(2016, 1, 4, 12, 45),
            buytype='BUY', secinfo=self.secinfo2,
            units=Decimal('100'), unitprice=Decimal('42.24'),
            commission=Decimal('9.99'), total=Decimal('-4233.99'),
            subacctsec='CASH', subacctfund='CASH'
        )

    def tearDown(self):
        pass
        try:
            os.unlink(dbfile)
        except OSError:  # file not created by test -- probably an error
            pass

    def testOfxLogGetOrCreate(self):
        with session_scope() as DBSession:
            ofxlog = OfxLog(invtran=self.invtran1)
            DBSession.add(ofxlog)
            # Already created
            a, created = OfxLog.get_or_create(DBSession, self.invtran1) 
            self.assertEqual(a, ofxlog)
            self.assertEqual(a.invtran, self.invtran1)
            self.assertEqual(created, False)

            # Not yet created
            b, created = OfxLog.get_or_create(DBSession, self.invtran2)
            self.assertEqual(b.invtran, self.invtran2)
            self.assertEqual(created, True)

            # Test all DB instances
            ofxlogs = DBSession.query(OfxLog).filter(
                OfxLog.invtran_id == INVTRAN.id
            ).order_by(INVTRAN.dttrade).all()
            self.assertEqual(len(ofxlogs), 2)
            self.assertIs(ofxlogs[0], a)
            self.assertIs(ofxlogs[1], b)


if __name__=='__main__':
    unittest.main()
