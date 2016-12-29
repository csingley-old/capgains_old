# coding: utf-8
"""
"""
# stdlib imports
import logging
from contextlib import contextmanager
from collections import defaultdict
from decimal import Decimal
from datetime import (datetime, timedelta) 
import math
import csv
import re

# 3rd party imports
import sqlalchemy
from sqlalchemy.ext.declarative import (
    as_declarative,
    declared_attr,
)
from sqlalchemy import (
    Column,
    Integer,
    String,
    Numeric,
    DateTime,
    Boolean,
    ForeignKey,
    CheckConstraint,
    or_,
)
from sqlalchemy.orm import (
    relationship,
    backref,
)

from ofxtools import ofxalchemy
from ofxtools.ofxalchemy.models import (
    INVACCTFROM,
    SECINFO,
    INVTRAN,
    INVBUYSELL,
    RETOFCAP,
    SPLIT,
    TRANSFER,
    INCOME,
    INVEXPENSE,
)


# Local imports


# Our base class needs its tables to share the same metata and class registry as
# ofxalchemy.models classes in order for relationships to succeed.
@as_declarative(
    metadata=ofxalchemy.models.Base.metadata,
    class_registry=ofxalchemy.models.Base._decl_class_registry,
)
class Base(object):
    """
    SQLAlchemy declarative base for model classes in this module.

    We reuse the handy __repr__ function from ofxalchemy.models.base, 
    but this module doesn't require all the magic of surrogate primary keys 
    and _fingerprint().
    """
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    __repr__ = ofxalchemy.models.Base.__repr__


class Lot(Base):
    """
    Tracker for the cost basis and holding period of a security in inventory,
    in order to match purchases with sales when calculating capital gains.

    Lots are opened (beginning a holding period) and closed (realizing a gain),
    usually by trades.  Each Lot instance has a many-to-1 "opener" relationship
    to the transaction (INVTRAN) that originally created it.  When closed, a Lot
    instance gets a many-to-1 "closer" relationship to the realizing INVTRAN.

    A Lot is related to a security and account.  These relationships are fixed
    and don't change over the life of a Lot.  If a position's security or
    account changes before it is closed (due to a reorg, account transfer, etc.)
    a new Lot is created, and the old Lot gets a 1-to-1 "successor"/"predecessor"
    relationship to the new Lot that succeeds it.  The new Lot gets a many-to-1
    "starter" relationship referring to the INVTRAN responsible for generating
    the new Lot (which will be the same INVTRAN as the opener for a
    newly-created position), and the old Lot gets a many-to-1 "ender"
    relationship to the same INVTRAN (which will be the same as the closer for
    a closed Lot).

    In order to support manually created Lots (or loaded from a dump file), not
    just OFX imports, we can't guarantee that opener/closer/starter
    INVTRANs exist in the database where we can look up their transaction
    dates, so each Lot instance records its own dtopen/dtclose/dtstart/dtend,
    which are copied from the relevant INVTRAN if available.

    Every Lot has non-NULL dtopen/dtstart (which will be identical if the
    related account/security hasn't changed).  An open Lot will have NULL
    dtclose/dtend, and it will have non-NULL predecessor if it was started
    via transfer/reorg.  A Lot that was ended by a reorg/transfer will have
    NULL dtclose and non-NULL dtend, as well as a non-NULL successor.

    An open Lot gets split into multiple Lots by subsequent transactions that
    close only part of the Lot, so each Lot records its own units
    rather than simply referring to its opening transaction.

    Various events (spinoffs, return of capital, etc.) can change the cost of
    an open Lot, so it needs to keep track of its own cost rather than simply
    referring to its opening transaction.

    Each Lot keeps track of whether it has been used as replacement
    shares when calculating wash sales.

    Events that change the cost of an open Lot can also generate capital
    gains without closing the Lot, so there is a one-to-many relationship
    between Lot and Gain.
    """
    id = Column(Integer, primary_key=True)
    dtopen = Column(DateTime, nullable=False)
    dtclose = Column(DateTime, default=None)
    dtstart = Column(DateTime, nullable=False)
    dtend = Column(DateTime, default=None)
    units = Column(Numeric, CheckConstraint('units <> 0'), nullable=False)
    cost = Column(Numeric, nullable=False)
    washcost = Column(Numeric, nullable=False, default=Decimal('0'))
    acctfrom_id = Column(Integer, ForeignKey(
        'invacctfrom.id', onupdate='CASCADE', ondelete='CASCADE'),
        nullable=False,
    )
    account = relationship('INVACCTFROM', backref=backref('lots'))
    secinfo_id = Column(Integer, ForeignKey(
        'secinfo.id', onupdate='CASCADE', ondelete='CASCADE'),
        nullable=False,
    )
    security = relationship('SECINFO', backref=backref('lots'))
    opener_id = Column(Integer, ForeignKey(
        'invtran.id', onupdate='CASCADE', ondelete='CASCADE'),
    )
    opener = relationship('INVTRAN', foreign_keys=[opener_id],
                          backref=backref('openedlots')
                         )
    closer_id = Column(Integer, ForeignKey(
        'invtran.id', onupdate='CASCADE', ondelete='CASCADE'),
                       )
    closer = relationship('INVTRAN', foreign_keys=[closer_id],
                          backref=backref('closedlots')
                         )
    starter_id = Column(Integer, ForeignKey(
        'invtran.id', onupdate='CASCADE', ondelete='CASCADE',
    ),)
    starter = relationship('INVTRAN', foreign_keys=[starter_id],
                           backref=backref('startedlots')
                          )
    ender_id = Column(Integer, ForeignKey(
        'invtran.id', onupdate='CASCADE', ondelete='CASCADE',
    ),)
    ender = relationship('INVTRAN', foreign_keys=[ender_id],
                         backref=backref('endedlots')
                        )
    predecessor_id = Column(Integer, ForeignKey(
        'lot.id', onupdate='CASCADE', ondelete='CASCADE'
    ), default=None,)
    predecessor = relationship('Lot', uselist=False,
                             backref=backref('successor', remote_side=[id]),
                            )

    __table_args__ = ( CheckConstraint('units * cost >= 0'),)

    @property
    def unitCost(self):
        return self.cost / self.units

    @classmethod
    def asOf(cls, DBSession, dtasof, account=None, security=None):
        """ """
        lots = DBSession.query(cls).filter(
            cls.dtstart <= dtasof,
            or_(cls.dtend == None, cls.dtend > dtasof),
        )
        if account:
            lots = lots.filter(cls.acctfrom_id == account.id)
        if security:
            lots = lots.filter(cls.secinfo_id == security.id)
        return lots.order_by(cls.dtopen, cls.id)

    @classmethod
    def longsAsOf(cls, DBSession, dtasof, account=None, security=None):
        """ """
        return cls.asOf(DBSession, dtasof, account, security).filter(
            cls.units > 0
        )

    csvFields = ('brokerid', 'acctid',
                 'ticker', 'secname', 'uniqueidtype', 'uniqueid',
                 'dtopen', 'units', 'cost', 'washcost')
    @classmethod
    def loadCsv(cls, DBSession, csvfile):
        """
        """
        with open(csvfile) as csvfile:
            # Treat the first row as column headers
            #reader = csv.DictReader(csvfile, cls.csvFields)
            reader = csv.DictReader(csvfile)
            lots = []
            for row in reader:
                # If a matching account already exists in the DB, use it.
                # Otherwise create one.
                account = INVACCTFROM.lookupByPk(
                    DBSession, brokerid=row['brokerid'], acctid=row['acctid'],
                )
                if account is None:
                    account = INVACCTFROM(
                        brokerid=row['brokerid'], acctid=row['acctid'],
                    )
                    DBSession.add(account)

                # If a matching SECINFO already exists in the DB, use it.
                # Otherwise create an OTHERINFO (we don't know what it is,
                # and it doesn't matter very much for our purposes)
                secinfo = SECINFO.lookupByPk(
                    DBSession, uniqueid=row['uniqueid'],
                    uniqueidtype=row['uniqueidtype'],
                )
                if secinfo is None:
                    secinfo = ofxalchemy.models.OTHERINFO(
                        uniqueidtype=row['uniqueidtype'],
                        uniqueid=row['uniqueid'],
                        secname=row['secname'],
                        ticker=row['ticker'],
                    )
                # Now use the SECINFO to look up or create the security.
                # Accept either datetime format or date (ISO 8601)
                # Or try to accept "December 09, 2015" format
                dtopen = row['dtopen'].strip()
                try:
                    dtopen = datetime.strptime(dtopen, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    try:
                        dtopen = datetime.strptime(dtopen, '%Y-%m-%d')
                    except ValueError:
                        dtopen = datetime.strptime(dtopen, '%B %d, %Y')

                lot = cls(account=account, security=secinfo,
                          dtstart=dtopen, dtopen=dtopen,
                          units=Decimal(row['units']),
                          cost=Decimal(row['cost']),
                          washcost = Decimal(row['washcost']),
                         )
                DBSession.add(lot)
                lots.append(lot)
            return lots

    @classmethod
    def dumpCsv(cls, DBSession, csvfile, dtasof=None, consolidate=False):
        """
        Column 1:  brokerid
        Column 2:  acctid
        Column 3:  ticker
        Column 4:  secname
        Column 5:  uniqueidtype (e.g. CUSIP or ISIN)
        Column 6:  uniqueid
        Column 7:  dtopen
        Column 8:  units
        Column 9:  cost
        Column 10: washcost
        """
        dtasof = dtasof or datetime.max
        with open(csvfile, 'w') as csvfile:
            csvwriter = csv.DictWriter(csvfile, cls.csvFields, delimiter=',')
            csvwriter.writeheader()
            lots = cls.asOf(DBSession, dtasof)
            if consolidate:
                # Consolidate Lots by account/secinfo, 
                # disregarding dtopen/washcost
                p = defaultdict(list)
                for lot in lots:
                    secinfo = lot.security
                    p[(lot.account, secinfo)].append((lot.units, lot.cost))
                for (account, secinfo), lots in p.items():
                    position = {'brokerid': account.brokerid,
                                'acctid': account.acctid,
                                'ticker': secinfo.ticker,
                                'secname': secinfo.secname,
                                'uniqueidtype': secinfo.uniqueidtype,
                                'uniqueid': secinfo.uniqueid,
                                'units': sum([l[0] for l in lots]),
                                'cost': sum([l[1] for l in lots]),
                               }
                    csvwriter.writerow(position)
            else:
                for lot in lots:
                    account = lot.account
                    secinfo = lot.security
                    position = {'brokerid': account.brokerid,
                                'acctid': account.acctid,
                                'ticker': secinfo.ticker,
                                'secname': secinfo.secname,
                                'uniqueidtype': secinfo.uniqueidtype,
                                'uniqueid': secinfo.uniqueid,
                                'dtopen': lot.dtopen,
                                'units': lot.units,
                                'cost': lot.cost,
                                'washcost': int(lot.washcost),
                               }
                    csvwriter.writerow(position)

    #@classmethod
    #def wipe(cls, DBSession):
        #""" """
        #DBSession.query(cls).delete()

    @classmethod
    def doInvtrans(cls, DBSession, dtstart=None, dtend=None):
        dtstart = dtstart or datetime.min
        dtend = dtend or datetime.max
        invtrans = DBSession.query(INVTRAN).filter(
            INVTRAN.dttrade >= dtstart,
            INVTRAN.dttrade <= dtend,
        ).order_by(INVTRAN.dttrade, INVTRAN.id)
        for invtran in invtrans:
            cls.doInvtran(DBSession, invtran)

    @classmethod
    def doInvtran(cls, DBSession, invtran):
        """ """
        # First check for broker quirks
        quirks = brokerquirks.get(invtran.acctfrom.brokerid, None)
        if quirks:
            method = quirks.get(type(invtran), None)
            if method:
                method(DBSession, invtran)
                return
        if isinstance(invtran, INVBUYSELL):
            cls.trade(DBSession, invtran)
        elif isinstance(invtran, RETOFCAP):
            cls.returnOfCapital(DBSession, invtran)
        elif isinstance(invtran, SPLIT):
            cls.split(DBSession, invtran)
        else:
            return


    @classmethod
    def trade(cls, DBSession, invtran):
        # This function only handles trades
        if not isinstance(invtran, ofxalchemy.models.INVBUYSELL):
            raise ValueError('%s is not a trade' % invtran)

        # First check if we've already processed this transaction
        DBSession.add(invtran)
        t, created = OfxLog.get_or_create(DBSession, invtran)
        if not created:
            return

        # FIXME - FIFO is hard coded, but this should be configurable.
        openLots = cls.asOf(DBSession, invtran.dttrade,
                            account=invtran.acctfrom, security=invtran.secinfo
                           ).filter(cls.units * invtran.units < 0)

        # Match incoming INVTRAN units to open Lot units
        # until one or the other runs out
        units = invtran.units

        logging.info("Trade - original INVTRAN units: %s" % units)

        for lot in openLots:
            if units == 0:
                break

            logging.info("  Remaining INVTRAN units: %s" % units)
            logging.info("  vs. Lot units: %s" % lot.units)

            if abs(units) >= abs(lot.units):
                # More incoming units than Lot units
                # Close the whole Lot and continue the loop
                units = units + lot.units
            else:
                # More Lot units than incoming units - split the Lot
                # Close the incoming units and leave the rest open
                unitsOpen = units + lot.units
                costOpen = unitsOpen * lot.unitCost
                lot.units = -units
                lot.cost -= costOpen
                openLot = cls(
                    account=lot.account, security=lot.security,
                    units=unitsOpen, cost=costOpen,
                    dtopen=lot.dtopen, opener=lot.opener,
                    dtstart=lot.dtstart, starter=lot.starter,
                )
                DBSession.add(openLot)
                DBSession.commit()

                units = Decimal('0') # Break the loop next time around

                logging.info("  INVTRAN units all used up - split Lot")
                logging.info("  Leftover Lot: %s" % openLot)

            lot.dtclose = invtran.dttrade 
            lot.closer = invtran
            lot.dtend = lot.dtclose
            lot.ender = lot.closer
            proceeds = lot.units / invtran.units * -invtran.total
            gain = Gain(proceeds=proceeds, lot=lot, transaction=invtran)
            DBSession.add(gain)

            logging.info("  Closed Lot: %s" % lot)
            logging.info("  Gain: %s (units=%s, cost=%s, value=%s"  % \
                         (gain, gain.units, gain.cost, gain.value)
                        )

        # If any more incoming INVTRAN units remain, open a new Lot with them.
        if units:
            newLot = cls(
                account=invtran.acctfrom, security=invtran.secinfo,
                units=units, cost=-invtran.total,
                dtopen=invtran.dttrade, opener=invtran,
                dtstart=invtran.dttrade, starter=invtran,
            )
            DBSession.add(newLot)


    @classmethod
    def returnOfCapital(cls, DBSession, invtran, checklog=True):
        # This function only handles RETOFCAP
        #if not isinstance(invtran, ofxalchemy.models.RETOFCAP):
            #raise ValueError('%s is not a return of capital' % invtran)

        # First check if we've already processed this transaction
        t, created = OfxLog.get_or_create(DBSession, invtran)
        if checklog and not created:
            return

        dtasof = invtran.dttrade
        lots = cls.longsAsOf(DBSession, dtasof, security=invtran.secinfo)
        totalUnits = sum([lot.units for lot in lots])
        try:
            assert totalUnits
        except AssertionError:
            logging.critical('dtasof=%s, secinfo=%s, invtran=%s - no units!' % (dtasof, invtran.secinfo.ticker, invtran.memo))
            raise
        unitRetofcap = invtran.total / totalUnits

        # Create new Lots as of the return of capital so that queries before/after
        # that date will return the correct cost at that time.
        for lot in lots:
            costAdj = lot.units * unitRetofcap
            adjCost = lot.cost - costAdj

            lot.ender = invtran
            lot.dtend = dtasof
            newLot = Lot(account=lot.account, security=lot.security,
                         units=lot.units, cost=adjCost,
                         washcost=lot.washcost, predecessor=lot,
                         opener=lot.opener, dtopen=lot.dtopen,
                         starter=invtran, dtstart=dtasof,
                        )
            DBSession.add(newLot)

            if  adjCost < 0:
                newLot.cost = 0
                gain = Gain(proceeds=-adjCost, lot=newLot, transaction=invtran)
                DBSession.add(gain)


    @classmethod
    def split(cls, DBSession, invtran):
        """ """
        # This function only handles SPLIT
        #if not isinstance(invtran, ofxalchemy.models.SPLIT):
            #raise ValueError('%s is not a split' % invtran)

        # Check if we've already processed this transaction
        t, created = OfxLog.get_or_create(DBSession, invtran)
        if not created:
            return

        ratio = invtran.newunits / invtran.oldunits

        # Sanity check the SPLIT
        if  ratio != invtran.numerator / invtran.denominator:
            raise ValueError('Inconsistent ratio vs. units for %s' % invtran)

        # List of Lots are subject to the split
        dtasof = invtran.dttrade
        lots = cls.asOf(DBSession, dtasof, security=invtran.secinfo)

        newUnits = Decimal('0')
        # Create new Lots as of the split date so that queries before/after
        # that date will return the correct units at that time.
        for lot in lots:
            lot.ender = invtran
            lot.dtend = dtasof
            newLot = Lot(account=lot.account, security=lot.security,
                         units=lot.units * ratio, cost=lot.cost,
                         washcost=lot.washcost, predecessor=lot,
                         opener=lot.opener, dtopen=lot.dtopen,
                         starter=invtran, dtstart=dtasof,
                        )
            DBSession.add(newLot)
            newUnits += newLot.units

        try:
            assert abs(newUnits - invtran.newunits) < Decimal('0.00000001')
        except AssertionError:
            logging.critical("ratio: %s, newUnits: %s, invtran.newunits: %s" % \
                  (ratio, newUnits, invtran.newunits))
            raise


class Gain(Base):
    """
    Capital gain realized by a transaction in a Lot of a security.

    Realized gains are not only generated by closing trades, but also by
    return of capital, etc.  Since the OFX data from brokerages simply provides
    aggregate totals not per-unit data, each Gain needs to keep track of the
    portion of the proceeds that are attributable to its own Lot.

    As well, Gains track the portion of their profit/loss that is disallowed
    due to a wash sale.
    """
    id = Column(Integer, primary_key=True)
    proceeds = Column(Numeric, nullable=False)
    washloss = Column(Numeric, nullable=False, default=Decimal('0'))
    lot_id = Column(Integer,
                    ForeignKey('lot.id',
                               onupdate='CASCADE', ondelete='CASCADE'),
                    nullable=False,
                   )
    lot = relationship('Lot', backref=backref('gains'))
    invtran_id = Column(Integer,
                        ForeignKey('invtran.id',
                                   onupdate='CASCADE', ondelete='CASCADE'),
                       )
    transaction = relationship('INVTRAN')

    @property
    def units(self):
        return self.lot.units

    @property
    def cost(self):
        return self.lot.cost

    @property
    def value(self):
        return self.proceeds - self.cost

    @property
    def washcost(self):
        return self.lot.washcost

    @property
    def taxcost(self):
        return self.cost + self.washcost

    @property
    def taxvalue(self):
        return self.proceeds - self.taxcost

    @property
    def dtopen(self):
        return self.lot.dtopen

    @property
    def dtclose(self):
        return self.transaction.dttrade

    @property
    def isLongTerm(self):
        # Short sales always generate STCG
        if self.units < 0:
            return False
        return (self.dtclose - self.dtopen).days > 365

    @classmethod
    def doWashSales(cls, DBSession, dtstart=None, dtend=None):
        """ """
        dtstart = dtstart or datetime.min
        dtend = dtend or datetime.max
        # We're looking for Gains on Lots closed during the period of interest,
        # where the gain hasn't already been disallowed.
        gains = DBSession.query(cls).filter(
            cls.washloss == 0,
            cls.lot_id == Lot.id,
            Lot.dtclose != None,
            Lot.dtopen > dtstart,
            Lot.dtopen <= dtend,
        ).order_by(Lot.dtopen)

        [ gain.doWashSale(DBSession) for gain in gains ]

    def doWashSale(self, DBSession):
        logging.info('Evaluating wash sale for %s(lot=%s, transaction=%s)' % \
                     (self, self.lot, self.transaction)
                    )

        # Open Lots can't have wash sales
        if not self.lot.dtclose: 
            # This case is excluded by the doWashSales() query that normally
            # calls doWashSale()
            logging.warn('Lot is still open: %s' % self.lot)
            return

        # If this Gain already has disallowed loss from a wash sale, it's
        # already been processed.
        if self.washloss:
            # This case is excluded by the doWashSales() query that normally
            # calls doWashSale()
            logging.warn('Loss already disallowed: %s' % self)
            return

        # Only realized tax losses are considered for wash sales
        if self.value >=0:
            # We'd like to exclude this case in the doWashSales() query,
            # but the INVTRAN superclass doesn't have a total column,
            # so we handle it here
            #logging.warn('Not a loss: %s' % self)
            logging.info('Not a loss: %s' % self)
            return

        # To be a wash sale, this Gain must be on a trade that closed a Lot.
        if self.lot.closer != self.transaction:
            logging.info('Not a closing transaction: %s' % self)
            return

        DBSession.add(self)

        # Identify replacement Lots (i.e. shares of the same security 
        # in the same account with the same sign opened within 
        # 30 days +/- the date the Gain was realized, which haven't
        # already been identified as replacement shares for another
        # wash sale)
        replacementLots = DBSession.query(Lot).filter(
            Lot.id != self.lot.id, 
            Lot.account == self.lot.account,
            Lot.security == self.lot.security,
            Lot.units * self.units > 0,
            Lot.washcost == 0,
            Lot.dtopen >= self.lot.dtclose - timedelta(days=30),
            Lot.dtopen <= self.lot.dtclose + timedelta(days=30),
        ).order_by(Lot.dtopen)

        # First tally the replacement Lots so we know how much loss to disallow
        # FIXME we should be able to get this by SUMming the query just above
        replacementUnits = sum([rl.units for rl in replacementLots])

        # If there are no replacement units, we can skip this whole rigmarole
        if replacementUnits == 0:
            logging.info('No replacement units found; no wash sale: %s' % self)
            return

        # Perform all calculations before altering any values on the
        # wash sale Gain or its Lot
        lot = self.lot

        # Sign-agnostic min(): take the lesser magnitude of
        # replacement units and loss units
        replacementUnits = Decimal(math.copysign(
            min(abs(replacementUnits), abs(lot.units)),
            replacementUnits
        ))

        washedUnits = replacementUnits
        unwashedUnits = lot.units - washedUnits

        washedCost = washedUnits * lot.unitCost
        unwashedCost = unwashedUnits * lot.unitCost

        unitProceeds = self.proceeds / lot.units
        washedProceeds = washedUnits * unitProceeds
        unwashedProceeds = unwashedUnits * unitProceeds

        unitLoss = self.value / lot.units
        disallowedLoss = washedUnits * unitLoss

        logging.info('Wash sale for %s' % self)
        logging.info('Wash sale: units=%s, cost=%s, proceeds=%s, loss=%s' % \
                     (washedUnits, washedCost, washedProceeds, disallowedLoss)
                    )
        logging.info('Not part of wash sale: units=%s, cost=%s, proceeds=%s' % \
                     (unwashedUnits, unwashedCost, unwashedProceeds)
                    )

        # Partition Gain units/cost between wash sale and excess
        logging.info('Setting units=%s, cost=%s for %s' % \
                     (washedUnits, washedCost, lot)
                    )
        lot.units = washedUnits
        lot.cost = washedCost

        # Fix the proceeds on the wash sale Gain (i.e. self) and disallow loss
        # FIXME - it's stupid to have washloss be a Decimal if we're always
        # going to set it equal to the loss value!
        logging.info('Setting wash sale proceeds=%s, washloss=%s for %s' % \
                     (washedProceeds, disallowedLoss, self)
                    )
        self.proceeds = washedProceeds
        try:
            assert abs(self.value - disallowedLoss) < Decimal('0.00000001')
        except AssertionError:
            logging.critical('gain.value=%s != disallowedLoss=%s' % \
                             (self.value, disallowedLoss)
                            )
            raise
        self.washloss = disallowedLoss

        # If there are more loss units than replacement units, create new loss
        # Lots/Gains # to divide units/cost/proceeds between washed & unwashed.
        if unwashedUnits:
            unwashedLot = Lot(
                account=lot.account, security=lot.security,
                units=unwashedUnits, cost=unwashedCost,
                dtstart=lot.dtstart, starter=lot.starter,
                dtend=lot.dtend, ender=lot.ender,
                dtopen=lot.dtopen, opener=lot.opener,
                dtclose=lot.dtclose, closer=lot.closer,
            )
            DBSession.add(unwashedLot)
            logging.info('Created new Lot not part of wash sale: %s' % unwashedLot)

            # We also need to split any *other* Gains (i.e. other than than self)
            # on the split loss Lot
            for gain in lot.gains:
                transaction = gain.transaction
                proceeds = gain.units / -transaction.units * transaction.total
                if gain is self:
                    # continue
                    try:
                        assert abs(proceeds-washedProceeds)<Decimal('0.00000001')
                    except AssertionError:
                        logging.critical('proceeds=%s != washedProceeds=%s' % \
                                         (proceeds, washedProceeds)
                                        )
                        raise
                gain.proceeds = proceeds
                logging.info('Adjusted wash sale proceeds=%s for %s' % \
                             (proceeds, gain)
                            )
                unwashedGain = Gain(
                    lot=unwashedLot,
                    transaction=gain.transaction,
                    #proceeds=unwashedLot.units / -transaction.units * transaction.total
                    proceeds=unwashedUnits / -transaction.units * transaction.total,
                )
                DBSession.add(unwashedGain)
                logging.info('Created new Gain not part of wash sale: %s' % unwashedGain)

        # Loop through all the replacement lots, marking them with cost
        # basis adjustments as we go.  If we run out of loss units partway
        # through a replacement Lot, that Lot (and any associated Gains)
        # will need to be split.
        for lot in replacementLots:
            if replacementUnits == 0:
                break

            logging.info('%s replacement units left; applying to %s' % \
                         (replacementUnits, lot)
                        )

            if abs(replacementUnits) >= abs(lot.units):
                # More Gain units remain than replacement Lot units.
                # Adjust the cost basis of the replacement Lot and keep going.
                washedUnits = lot.units

                lot.washcost = washedUnits * -unitLoss
                logging.info('Rolled washcost=%s into cost basis of %s' % \
                             (lot.washcost, lot)
                            )
            else:
                # More replacement Lot units than Gain units.
                # This is the end of the line for the loop.
                #
                # Perform all calculations before altering any values on the
                # replacement Gain or its Lot
                washedUnits = replacementUnits
                washedCost = washedUnits * lot.unitCost
                unwashedUnits = lot.units - washedUnits
                unwashedCost = unwashedUnits * lot.unitCost

                # Split the replacement Lot into washed/unwashed.
                # Adjust the cost basis of the washed replacement Lot.
                # Allocate replacement Lot cost basis proportionately
                # between washed & unwashed replacement Lots.
                lot.units = washedUnits
                lot.cost = washedCost
                lot.washcost = washedUnits * -unitLoss
                logging.info('Segregated replacement shares; units=%s, cost=%s; rolled washcost=%s into cost basis of %s' % (washedUnits, washedCost, lot.washcost, lot)
                            )

                unwashedLot = Lot(
                    account=lot.account, security=lot.security,
                    units=unwashedUnits, cost=unwashedCost,
                    dtstart=lot.dtstart, starter=lot.starter,
                    dtend=lot.dtend, ender=lot.ender,
                    dtopen=lot.dtopen, opener=lot.opener,
                    dtclose=lot.dtclose, closer=lot.closer,
                )
                DBSession.add(unwashedLot)
                logging.info('Segregated non-replacement shares; units=%s, cost=%s: %s' % \
                             (unwashedLot.units, unwashedLot.cost, lot)
                            )

                # If the replacement Lot has Gains, those also need to be
                # split between washed & unwashed Lots with proceeds allocated
                # proportionately.
                for gain in lot.gains:
                    transaction = gain.transaction
                    gain.proceeds = gain.units / -transaction.units * transaction.total
                    logging.info('Gain on replacement Lot: adjusted proceeds=%s on replacement units: %s' % (gain.proceeds, gain)
                                )
                    unwashedGain = Gain(
                        lot=unwashedLot,
                        transaction=gain.transaction,
                        proceeds=unwashedLot.units / -transaction.units * transaction.total
                    )
                    DBSession.add(unwashedGain)
                    logging.info('Segregated proceeds=%s as Gain on non-replacement units: %s' % (unwashedGain.proceeds, unwashedGain))

            replacementUnits -= washedUnits
            disallowedLoss += lot.washcost

        assert replacementUnits == Decimal('0')
        assert disallowedLoss == Decimal('0')

    csvFields = ('brokerid', 'acctid', 'ticker', 'secname', 'dtclose',
                 'fitidclose', 'longterm', 'dtopen', 'fitidopen', 'units',
                 'proceeds', 'cost', 'gain', 'washcost', 'washloss',)
    @classmethod
    def dumpCsv(cls, DBSession, csvfile, dtstart=None, dtend=None, 
                account=None, security=None, consolidate=False):
        """ """
        dtstart = dtstart or datetime.min
        dtend = dtend or datetime.max
        with open(csvfile, 'w') as csvfile:
            csvwriter = csv.DictWriter(csvfile, cls.csvFields, delimiter=',')
            csvwriter.writeheader()

            gains = DBSession.query(cls).filter(
                cls.invtran_id == INVTRAN.id,
                INVTRAN.dttrade >= dtstart,
                INVTRAN.dttrade <= dtend,
            )
            if account:
                lots = lots.filter_by(account=account)
            if security:
                lots = lots.filter_by(security=security)

            if consolidate:
                # Consolidate Gains by account/secinfo, 
                # disregarding dtopen/dtclose
                p = defaultdict(list)
                for gain in gains:
                    p[(gain.lot.account, gain.lot.security)].append((gain.units, gain.proceeds, gain.cost, gain.value, gain.washcost, gain.washloss))
                for (account, secinfo), gains in p.items():
                    secgain = {'brokerid': account.brokerid,
                               'acctid': account.acctid,
                               'ticker': secinfo.ticker,
                               'secname': secinfo.secname,
                               'units': sum([g[0] for g in gains]),
                               'proceeds': sum([g[1] for g in gains]),
                               'cost': sum([g[2] for g in gains]),
                               'gain': sum([g[3] for g in gains]),
                               'washcost': sum([g[4] for g in gains]),
                               'washloss': sum([g[5] for g in gains]),
                              }
                    csvwriter.writerow(secgain)
            else:
                for gain in gains:
                    lot = gain.lot
                    account = lot.account
                    secinfo = lot.security
                    opener = lot.opener

                    if opener:
                        fitidopen = opener.fitid
                    else:
                        fitidopen = None

                    if gain.isLongTerm:
                        longterm = 'LTCG'
                    else:
                        longterm = 'STCG'
                    g = {'brokerid': account.brokerid, 'acctid': account.acctid,
                         'ticker': secinfo.ticker, 'secname': secinfo.secname,
                         'dtclose': gain.dtclose,
                         'fitidclose': gain.transaction.fitid,
                         'dtopen': gain.dtopen, 'fitidopen': fitidopen,
                         'longterm': longterm,
                         'units': gain.units, 'proceeds': gain.proceeds,
                         'cost': gain.cost, 'gain': gain.value,
                         'washcost': gain.washcost, 'washloss': gain.washloss,
                        }
                    csvwriter.writerow(g)


class OfxLog(Base):
    """ """
    id = Column(Integer, primary_key=True)
    invtran_id = Column(Integer, ForeignKey(
        'invtran.id', onupdate='CASCADE', ondelete='CASCADE',)
        , unique=True)
    invtran = relationship('INVTRAN')

    @classmethod
    def get_or_create(cls, DBSession, invtran):
        """ """
        t = DBSession.query(cls).filter_by(invtran=invtran).one_or_none()
        if t:
            created = False
        else:
            t = cls(invtran=invtran)
            DBSession.add(t)
            created = True
        return t, created


class IBKR(object):
    transferMemoRE = re.compile(
        r"""
        (?P<memo>.+)
        \s+
        \( (?P<ticker>.+), \s+ (?P<secname>.+), \s+ (?P<uniqueid>[\w]+) \)
        """, re.VERBOSE | re.IGNORECASE
    )

    retofcapMemoRE = re.compile(
        r"""
        (?P<memo>.+)
        \s+
        \(Return\ of\ Capital\)
        """, re.VERBOSE | re.IGNORECASE
    )

    @classmethod
    def doTransfer(cls, DBSession, invtran):
        """ """
        # This method only works on TRANSFER
        if not isinstance(invtran, ofxalchemy.models.TRANSFER):
            raise ValueError('%s is not a transfer' % invtran)

        # First check if we've already processed this transaction
        #DBSession.add(invtran)
        t, created = OfxLog.get_or_create(DBSession, invtran)
        if not created:
            return
        

        match = cls.transferMemoRE.match(invtran.memo)
        # Sanity check memo regex match
        assert match
        #assert match.group('ticker') == invtran.secinfo.ticker
        assert match.group('uniqueid') == invtran.secinfo.uniqueid
        memo = match.group('memo')
        assert memo

        twin = DBSession.query(TRANSFER).filter(
            TRANSFER.id != invtran.id, TRANSFER.acctfrom == invtran.acctfrom,
            TRANSFER.dttrade == invtran.dttrade, TRANSFER.memo.like(memo+'%'),
        ).one_or_none()

        if twin:
            t, created = OfxLog.get_or_create(DBSession, twin) 
            assert created
            # The TRANSFER pair should have opposite signs
            assert invtran.units * twin.units < 0

            # Which TRANSFER (passed in or looked-up twin) corresponds to
            # a security for which we already own Lots?
            security = invtran.secinfo
            lots = Lot.asOf(DBSession, invtran.dttrade,
                            account=invtran.acctfrom, security=security).all()

            if lots and invtran.units == -sum([lot.units for lot in lots]):
                units = sum([lot.units for lot in lots])
                transferOut, transferIn = invtran, twin
                newSecurity = twin.secinfo
            else:
                # Do we own the looked-up twin TRANSFER?
                newSecurity = invtran.secinfo 
                security = twin.secinfo
                lots = Lot.asOf(DBSession, invtran.dttrade,
                                account=invtran.acctfrom, security=security
                               ).all()
                if not lots:
                    # We don't own either side of the pair; ignore it
                    return
                units = sum([lot.units for lot in lots])
                try:
                    assert units == -twin.units
                except AssertionError:
                    logging.critical("Inventory Lot: ticker=%s, uniqueid=%s, units=%s; incoming units=%s from transaction dttrade=%s fitid=%s" % (twin.secinfo.ticker, twin.secinfo.uniqueid, units, twin.units, twin.dttrade, twin.fitid))
                    raise
                transferOut, transferIn = twin, invtran

            ratio = transferIn.units / units
            for lot in lots:
                lot.ender = transferOut
                lot.dtend = invtran.dttrade
                newLot = Lot(
                    account=lot.account, security=newSecurity,
                    units=lot.units * ratio, cost=lot.cost,
                    washcost=lot.washcost,
                    dtopen=lot.dtopen, opener=lot.opener,
                    dtstart=invtran.dttrade, starter=transferIn,
                    predecessor=lot,
                )
                DBSession.add(newLot)

    @classmethod
    def doIncome(cls, DBSession, invtran):
        """ """
        # This method only works on INCOME
        if not isinstance(invtran, ofxalchemy.models.INCOME):
            raise ValueError('%s is not an income' % invtran)

        # First check if we've already processed this transaction
        t, created = OfxLog.get_or_create(DBSession, invtran)
        if not created:
            return
        
        # IBKR books return of capital as INCOME rather than RETOFCAP,
        # only noting this classification in the memo field
        if 'return of capital' not in invtran.memo.lower():
            return
        match = cls.retofcapMemoRE.match(invtran.memo)
        assert match
        memo = match.group('memo')

        # Before changing Lot cost basis, check to see that the return of
        # capital hasn't been reversed - this happens often as the broker
        # first books part of the cash as payment in lieu, then later sorts
        # out the hypothecation and rebooks as return of  capital.
        #
        # INCOME transactions get reversed as INVEXPENSE, so check those
        # for matching date/total/memo.
        reversal = DBSession.query(INVEXPENSE).filter(
            INVEXPENSE.dttrade == invtran.dttrade,
            INVEXPENSE.total == -invtran.total,
            INVEXPENSE.memo.like(memo+'%'),
        ).order_by(INVEXPENSE.id).first()
        if reversal:
            t, created = OfxLog.get_or_create(DBSession, reversal)
            return

        # Process this INCOME as a RETOFCAP.
        # We created an OfxLog instance above for invtran;
        # disable OfxLog checks in Lot.returnOfCapital() (which would
        # flag our INCOME as already processed)
        Lot.returnOfCapital(DBSession, invtran, checklog=False)


brokerquirks = {
    '4705': {TRANSFER: IBKR.doTransfer, INCOME: IBKR.doIncome,},
}


@contextmanager
def session_scope(database):
    """
    Provide a transactional scope around a series of database operations.
    """
    session = Session(database)
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def Session(database):
    engine = sqlalchemy.create_engine(database)
    ofxalchemy.models.Base.metadata.create_all(engine)
    Base.metadata.create_all(engine)
    return sqlalchemy.orm.sessionmaker(bind=engine)()


def ofximport(args):
    for file in args.file:
        ofxparser = ofxalchemy.OFXTree()
        ofxparser.parse(file)
        with session_scope(args.database) as session:
            ofxparser.instantiate(session)


def loadCsv(args):
    for file_ in args.file:
        with session_scope(args.database) as session:
            lots = Lot.loadCsv(session, file_)


def calc(args):
    dtstart = args.dtstart
    dtend = args.dtend
    if dtstart:
        dtstart = datetime.strptime(args.dtstart, '%Y-%m-%d')
    if dtend:
        dtend = datetime.strptime(args.dtend, '%Y-%m-%d')

    with session_scope(args.database) as session:
        Lot.doInvtrans(session, dtstart=dtstart, dtend=dtend)
        Gain.doWashSales(session, dtstart=dtstart, dtend=dtend)


def dump(args):
    dtasof = args.dtasof
    if dtasof:
        dtasof = datetime.strptime(args.dtasof, '%Y-%m-%d')

    with session_scope(args.database) as session:
        Lot.dumpCsv(session, args.file, dtasof=args.dtasof,
                    consolidate=args.consolidate,
                   )

def gain(args):
    dtstart = args.dtstart
    if dtstart:
        dtstart = datetime.strptime(args.dtasof, '%Y-%m-%d')
    dtend = args.dtend
    if dtend:
        dtend = datetime.strptime(args.dtasof, '%Y-%m-%d')

    with session_scope(args.database) as session:
        Gain.dumpCsv(session, args.file, dtstart=args.dtstart, dtend=args.dtend,
                     consolidate=args.consolidate,
                    )


def make_argparser():
    """
    Return subparsers as well, so that the ArgumentParser can be extended.
    """
    from argparse import ArgumentParser

    argparser = ArgumentParser(description='Lot utility')
    argparser.add_argument('-d', '--database', default='sqlite://',
                           help='Database connection')
    argparser.add_argument('--verbose', '-v', action='count',
                           help='-vv for DEBUG')
    argparser.set_defaults(func=None)
    subparsers = argparser.add_subparsers()
    import_parser = subparsers.add_parser('import', help='Import OFX file(s)')
    import_parser.add_argument('file', nargs='+', help='OFX file(s)')
    import_parser.set_defaults(func=ofximport)
    load_parser = subparsers.add_parser('load', help='Load CSV file(s)')
    load_parser.add_argument('file', nargs='+', help='CSV file(s)')
    load_parser.set_defaults(func=loadCsv)
    calc_parser = subparsers.add_parser('calc', help='Calculate Lots and Gains')
    calc_parser.set_defaults(func=calc, dtstart=None, dtend=None)
    dump_parser = subparsers.add_parser('lots', aliases=['dump'],
                                        help='Dump Lots to CSV file')
    dump_parser.add_argument('file', help='CSV file')
    dump_parser.add_argument('-a', '--dtasof', nargs='?')
    dump_parser.add_argument('-c', '--consolidate', action='store_true') 
    dump_parser.set_defaults(func=dump, dtasof=None)
    gain_parser = subparsers.add_parser('gains', help='Dump Gains to CSV file')
    gain_parser.add_argument('file', help='CSV file')
    gain_parser.add_argument('-s', '--dtstart', default=None)
    gain_parser.add_argument('-e', '--dtend', default=None)
    gain_parser.add_argument('-c', '--consolidate', action='store_true') 
    gain_parser.set_defaults(func=gain)

    return argparser, subparsers


def main():
    argparser, subparsers = make_argparser()
    args = argparser.parse_args()

    # Set logging level
    loglevel = {1: getattr(logging, 'INFO'), 2: getattr(logging, 'DEBUG')}.get(
        args.verbose, getattr(logging, 'WARNING')
    )
    logging.basicConfig(level=loglevel)

    # Execute specified command
    if args.func:
        args.func(args)
    else:
        argparser.print_help()


if __name__ == '__main__':
    main()
