# -*- coding: utf-8 -*-
# Copyright (c) 2004-2014 Alterra, Wageningen-UR
# Allard de Wit (allard.dewit@wur.nl), April 2014
import numpy as np

from ..traitlets import Bool, Float, List
from .. import signals
from ..base_classes import StatesTemplate, SimulationObject, prepare_states


class AgroMetIndicators(SimulationObject):
    _GSRAINSUM = List()
    _GSTEMPAVG = List()
    _GSRADIATIONSUM = List()

    class StateVariables(StatesTemplate):
        GSRAINSUM = Float()
        GSTEMPAVG = Float()
        GSRADIATIONSUM = Float()


    def initialize(self, day, kiosk, parvalues):

        self.states = self.StateVariables(kiosk, GSRAINSUM=0,
                                          GSTEMPAVG=0, GSRADIATIONSUM=0)
        self._GSRAINSUM = []
        self._GSTEMPAVG = []
        self._GSRADIATIONSUM = []

    def calc_rates(self, day, drv):
        self._GSRAINSUM.append(drv.RAIN)
        self._GSTEMPAVG.append(drv.TEMP)
        self._GSRADIATIONSUM.append(drv.IRRAD)

    def integrate(self, day):
        pass

    @prepare_states
    def finalize(self, day):
        self.states.GSRAINSUM = np.sum(self._GSRAINSUM)
        self.states.GSRADIATIONSUM = np.sum(self._GSRADIATIONSUM)
        self.states.GSTEMPAVG = np.sum(self._GSTEMPAVG)
