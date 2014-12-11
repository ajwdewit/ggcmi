# -*- coding: utf-8 -*-
# Copyright (c) 2004-2014 Alterra, Wageningen-UR
# Allard de Wit (allard.dewit@wur.nl), April 2014
import numpy as np
import pdb

from ..traitlets import Bool, Float, List
from .. import signals
from ..base_classes import StatesTemplate, SimulationObject, prepare_states


class AgroMetIndicators(SimulationObject):
    _GSRAINSUM = List()
    _GSTEMPSUM = List()
    _GSRADIATIONSUM = List()

    class StateVariables(StatesTemplate):
        GSRAINSUM = Float()
        GSTEMPSUM = Float()
        GSRADIATIONSUM = Float()


    def initialize(self, day, kiosk, parvalues):

        self.states = self.StateVariables(kiosk, GSRAINSUM=0,
                                          GSTEMPSUM=0, GSRADIATIONSUM=0)
        self._GSRAINSUM = []
        self._GSTEMPSUM = []
        self._GSRADIATIONSUM = []

    def calc_rates(self, day, drv):
        self._GSRAINSUM.append(drv.RAIN)
        self._GSTEMPSUM.append(drv.TEMP)
        self._GSRADIATIONSUM.append(drv.IRRAD)

    def integrate(self, day):
        pass

    @prepare_states
    def finalize(self, day):
        self.states.GSRAINSUM = np.sum(self._GSRAINSUM)
        self.states.GSRADIATIONSUM = np.sum(self._GSRADIATIONSUM)
        self.states.GSTEMPSUM = np.sum(self._GSTEMPSUM)

