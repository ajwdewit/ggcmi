"""This module contains code that wrap the FORTRAN77 routines of WOFOST6

Classes defined here:
* py_W60lib_wrapper - This class is only meant to be subclassed and not to be
                      used directly as it contains no public methods.
"""

import sys, os
import copy
import datetime
import warnings

from ..wofosttimer import Timer
from ..util import make_len30

if sys.platform == 'darwin':
    import py_w60lib_darwin as w60lib
elif sys.platform == 'linux2':
    import py_w60lib_linux2 as w60lib
elif sys.platform == 'win32':
    import py_w60lib_win32 as w60lib

#-------------------------------------------------------------------------------
class PyContainer(object):
    # Empty container for storing attributes
    pass

#-------------------------------------------------------------------------------
class py_W60lib_wrapper(object):
    
    # DELT defines the timestep of the model. WOFOST can run with different
    # settings for DELT (1,10,30) but for PyWofost we just hardcode DELT=1
    DELT = 1
    
    # Disable Oxygen shortage in root zone by default
    IOX = 0

    #---------------------------------------------------------------------------
    def _set_crop_calendar(self, timerdata):
        """Determines options for starting/ending the model based on timerdata.
        
        The following options are determined:
        - Start of the water balance
        - Model start at FIXED_SOWING or FIXED_EMERGENCE
        - Model end at MATURITY or FIXED_HARVEST
        """
        
        # Create an container for returning results
        c = PyContainer()

        # Get the logger object
        logger = self.logger

        # Determine start of simulation, assuming the waterbalance starts
        # earlier or at the same time as the crop simulation
        start_date_simulation = timerdata["WB_START_DATE"]
        start_date_crop = timerdata["CROP_START_DATE"]
        self.NOCROP = 0
        if (start_date_simulation < start_date_crop):
            self.NOCROP = 1
        logger.debug("%s Pywofost simulation to be started" %
                      start_date_simulation)
        logger.debug("Start date of crop = %s" % start_date_crop)
        logger.debug("NOCROP = %s" % self.NOCROP)

        # Determine whether model starts at sowing or at emergence
        if (timerdata["CROP_START_TYPE"] == "FIXED_EMERGENCE"):
            day_of_emergence = start_date_crop
            self.IDEM = (day_of_emergence - start_date_simulation).days + 1 
            self.ISTATE = 3
            logger.debug("Start type = Fixed Emergence at %s" %
                         day_of_emergence)
        elif (timerdata["CROP_START_TYPE"] == "FIXED_SOWING"):
            self.IDEM = -99
            self.ISTATE = 1
            logger.debug("Start type = Fixed Sowing at %s" % start_date_crop)
        else:
            logger.error("Unknown start option, aborting PyWofost run!")
            raise RuntimeError

        #Set options for maximum duration of simulation
        if (timerdata["CROP_END_TYPE"] == "HARVEST"):
            end_date_simulation = timerdata["CROP_END_DATE"]
            logger.debug("End type = Fixed Harvest at %s" % \
                         end_date_simulation)
        elif (timerdata["CROP_END_TYPE"] == "MATURITY"):
            max_duration = datetime.timedelta(days=timerdata["MAX_DURATION"])
            end_date_simulation = start_date_crop + max_duration
            logger.debug("End type = Maturity with Maximum Duration of %s days" \
                         % max_duration)
            # Note max_duration only used when maturity is not reached.
        else:
            logger.error("No valid option specified for end of simulation " +\
                         "aborting PyWofost run!")
            raise RuntimeError
        
        c.start_date_simulation = start_date_simulation
        c.start_date_crop = start_date_crop
        c.end_date_simulation = end_date_simulation
        return c
        
#-------------------------------------------------------------------------------
    def _init_COMMON_blocks(self, cropdata, soildata, sitedata):
        # Initialises COMMON blocks for crop, soil and site parameters.

        # Get the logger object
        logger = self.logger

        # Set crop variables in CROPVAR COMMON block
        w60lib.cropvar.rri = cropdata["RRI"]
        w60lib.cropvar.idsl = cropdata["IDSL"]
        w60lib.cropvar.dlo = cropdata["DLO"]
        w60lib.cropvar.dlc = cropdata["DLC"]
        w60lib.cropvar.tsum1 = cropdata["TSUM1"]
        w60lib.cropvar.tsum2 = cropdata["TSUM2"]
        w60lib.cropvar.dtsmtb = make_len30(cropdata["DTSMTB"])
        w60lib.cropvar.ildtsm = len(cropdata["DTSMTB"])
        w60lib.cropvar.dvsi = cropdata["DVSI"]
        w60lib.cropvar.dvsend = cropdata["DVSEND"]
        w60lib.cropvar.tdwi = cropdata["TDWI"]
        w60lib.cropvar.rgrlai = cropdata["RGRLAI"]
        w60lib.cropvar.slatb = make_len30(cropdata["SLATB"])
        w60lib.cropvar.ilsla = len(cropdata["SLATB"])
        w60lib.cropvar.spa = cropdata["SPA"]
        w60lib.cropvar.ssatb = make_len30(cropdata["SSATB"])
        w60lib.cropvar.ilssa = len(cropdata["SSATB"])
        w60lib.cropvar.span = cropdata["SPAN"]
        w60lib.cropvar.tbase = cropdata["TBASE"]
        w60lib.cropvar.kdiftb = make_len30(cropdata["KDIFTB"])
        w60lib.cropvar.ilkdif = len(cropdata["KDIFTB"])
        w60lib.cropvar.efftb = make_len30(cropdata["EFFTB"])
        w60lib.cropvar.ileff = len(cropdata["EFFTB"])
        w60lib.cropvar.amaxtb = make_len30(cropdata["AMAXTB"])
        w60lib.cropvar.ilamax = len(cropdata["AMAXTB"])
        w60lib.cropvar.tmpftb = make_len30(cropdata["TMPFTB"])
        w60lib.cropvar.iltmpf = len(cropdata["TMPFTB"])
        w60lib.cropvar.tmnftb = make_len30(cropdata["TMNFTB"])
        w60lib.cropvar.iltmnf = len(cropdata["TMNFTB"])
        w60lib.cropvar.cvl = cropdata["CVL"]
        w60lib.cropvar.cvo = cropdata["CVO"]
        w60lib.cropvar.cvr = cropdata["CVR"]
        w60lib.cropvar.cvs = cropdata["CVS"]
        w60lib.cropvar.q10 = cropdata["Q10"]
        w60lib.cropvar.rml = cropdata["RML"]
        w60lib.cropvar.rmo = cropdata["RMO"]
        w60lib.cropvar.rmr = cropdata["RMR"]
        w60lib.cropvar.rms = cropdata["RMS"]
        w60lib.cropvar.rfsetb = make_len30(cropdata["RFSETB"])
        w60lib.cropvar.ilrfse = len(cropdata["RFSETB"])
        w60lib.cropvar.frtb = make_len30(cropdata["FRTB"])
        w60lib.cropvar.ilfr = len(cropdata["FRTB"])
        w60lib.cropvar.fltb = make_len30(cropdata["FLTB"])
        w60lib.cropvar.ilfl = len(cropdata["FLTB"])
        w60lib.cropvar.fstb = make_len30(cropdata["FSTB"])
        w60lib.cropvar.ilfs = len(cropdata["FSTB"])
        w60lib.cropvar.fotb = make_len30(cropdata["FOTB"])
        w60lib.cropvar.ilfo = len(cropdata["FOTB"])
        w60lib.cropvar.perdl = cropdata["PERDL"]
        w60lib.cropvar.rdrrtb = make_len30(cropdata["RDRRTB"])
        w60lib.cropvar.ilrdrr = len(cropdata["RDRRTB"])
        w60lib.cropvar.rdrstb = make_len30(cropdata["RDRSTB"])
        w60lib.cropvar.ilrdrs = len(cropdata["RDRSTB"])
        w60lib.cropvar.cfet = cropdata["CFET"]
        w60lib.cropvar.depnr = cropdata["DEPNR"]
        w60lib.cropvar.iairdu = cropdata["IAIRDU"]
        w60lib.cropvar.rdi = cropdata["RDI"]
        w60lib.cropvar.rdmcr = cropdata["RDMCR"]
        w60lib.cropvar.tbasem = cropdata["TBASEM"]
        w60lib.cropvar.teffmx = cropdata["TEFFMX"]
        w60lib.cropvar.tsumem = cropdata["TSUMEM"]
        logger.debug("CROPVAR: COMMON block initialized")

        # Set soil variables in SOILVAR COMMON block
        w60lib.soilvar.sm0   = soildata["SM0"]
        w60lib.soilvar.smfcf = soildata["SMFCF"]
        w60lib.soilvar.smw   = soildata["SMW"]
        w60lib.soilvar.crairc = soildata["CRAIRC"]
        w60lib.soilvar.sope = soildata["SOPE"]
        w60lib.soilvar.ksub = soildata["KSUB"]
        w60lib.soilvar.k0   = soildata["K0"]
        logger.debug("SOILVAR: COMMON block initialized")

        # Set site variables in SITEVAR COMMON Block
        w60lib.sitevar.ifunrn = sitedata["IFUNRN"]
        w60lib.sitevar.ssmax = sitedata["SSMAX"]
        w60lib.sitevar.wav = sitedata["WAV"]
        # Note RDMSOL is a SITE variable in wof 7.1.2, now in soil variables
        w60lib.sitevar.rdmsol = soildata["RDMSOL"]  
        w60lib.sitevar.notinf = sitedata["NOTINF"]
        w60lib.sitevar.ssi = sitedata["SSI"]
        logger.debug("SITEVAR: COMMON block initialized")

#-------------------------------------------------------------------------------
    def _timer_init(self, start_date_simulation, end_date_simulation,
                    start_date_crop):
        
        # Get the logger object
        logger = self.logger

        # Set timer options and initialise Timer Class
        timer = Timer(start_date_simulation, end_date_simulation,
                      start_date_crop)
        # Make one call to timer to retrieve the initial state
        r = timer()
        [self.day, self.TIME, self.DOY, self.YEAR,
         self.outflag, self.time_termnl] = r
        logstr = ("Timer: Routine initialized. Output values for "+\
                  "day, DOY, YEAR, outflag, "+\
                  "time_termnl: %s, %s, %s, %s, %s") % \
                  (self.day, self.DOY, self.YEAR, self.outflag,
                   self.time_termnl)
        logger.debug(logstr)
        return timer

#-------------------------------------------------------------------------------
    def _CROPSI_init(self):
    
        # Get the logger object
        logger = self.logger
    
        # Set ITASK state for initialisation of FORTRAN routines
        ITASK = 1
    
        # Set dummy variable that will be passed in to variable positions
        # that have no function during initialisation
        d = 0

        #Initialise CROPSI Routine
        CROP_TERMNL = 0
        r = w60lib.cropsi(ITASK, self.DOY, self.DELT, self.TIME, self.IDEM, d,
                          d, CROP_TERMNL, self.ISTATE, d, d, d, d, d, d, d,
                          d, d, d, d, d, d, d)
        [self.IDEM, self.DOANTH, self.IDHALT, self.CROP_TERMNL,
         self.ISTATE, EVWMX, EVSMX, TRA, FR] = r
        logstr = ("CROPSI: Routine initialized with return values for "+\
                  "IDEM, DOANTH, IDHALT, CROP_TERMNL, ISTATE, EVWMX, "+\
                  "EVSMX, TRA, FR: %s, %s, %s, %s, %s, %s, %s, %s, %s") % \
                  (self.IDEM, self.DOANTH, self.IDHALT, self.CROP_TERMNL,
                   self.ISTATE, EVWMX, EVSMX, TRA, FR)
        logger.debug(logstr)
        
        # Add crop transpiration (TRA) to self. This is only to ensure that the
        # get_attribute() method can reach the value of this variable in both
        # idle and active runtime state.
        self.TRA = TRA
        
        return (EVWMX, EVSMX, TRA, FR)

#-------------------------------------------------------------------------------
    def _ROOTD_init(self, simulation_mode):
    
        # Get the logger object
        logger = self.logger
    
        # Set ITASK state for initialisation of FORTRAN routines
        ITASK = 1
    
        # Set dummy variable that will be passed in to variable positions
        # that have no function during initialisation
        d = 0
        
        # Determine mode of water balance
        IWB = 0
        if simulation_mode.lower() == 'wlp': IWB = 1

        #Initialise rooting depth
        r = w60lib.rootd(ITASK, self.DELT, IWB, d, d, d, d)
        [self.ZT, self.RDM, RD] = r
        logstr = ("ROOTD: Routine initialized with return values for "+\
                  "ZT, RDM, RD: %s, %s, %s") % (self.ZT, self.RDM, RD)
        logger.debug(logstr)
        
        # Add rooting depth (RD) to self. This is only to ensure that the
        # get_attribute() method can reach the value of this variable in both
        # idle and active runtime state.
        self.RD = RD

        return RD

#-------------------------------------------------------------------------------
    def _WATPP_init(self):
    
        # Get the logger object
        logger = self.logger
        
        # set IWB to 0
        self.IWB = 0
    
        # Set ITASK state for initialisation of FORTRAN routines
        ITASK = 1
    
        # Set dummy variable that will be passed in to variable positions
        # that have no function during initialisation
        d = 0
        
        # Value for IAIRDU (absence/presence of root airducts) can be derived
        # from COMMON block cropvar
        self.IAIRDU = w60lib.cropvar.iairdu

        #initialise water balance routines for potential production
        SM = w60lib.watpp(ITASK, self.DELT, self.IAIRDU, d, d, d, d)
        logger.debug("WATPP: Routine initialized with return values for "+\
                     "SM: %s" % SM)

        # Set profile soil moisture equal to root-zone soil moisture and
        # add variables PMC/RMC to self. The latter is needed for the
        # get_variable() method to find them.
        self.RMC = SM
        self.PMC = SM
        
        return SM

#-------------------------------------------------------------------------------
    def _WATFD_init(self):
    
        # Get the logger object
        logger = self.logger
    
        # set IWB to 1
        self.IWB = 1
    
        # Set ITASK state for initialisation of FORTRAN routines
        ITASK = 1
        
        # Set dummy variable that will be passed in to variable positions
        # that have no function during initialisation
        d = 0
        
        # Value for IAIRDU (absence/presence of root airducts) can be derived
        # from COMMON block cropvar
        self.IAIRDU = w60lib.cropvar.iairdu
        
        SM = w60lib.watfd(ITASK, self.DELT, self.IAIRDU, self.IDEM,
                             self.IDHALT, self.RDM, self.RD, d, d, d,
                             d, d)
        logger.debug("WATFD: Routine initialized with return values for "+\
                     "SM: %s" % SM)
        
        return SM

#------------------------------------------------------------------------------------------
    def _save_COMMON_blocks(self):
        """This method copies the FORTRAN COMMON blocks in the py_w60lib FORTRAN
        object into the WOFOST object. This is necessary because the FORTRAN
        objects in py_w60lib are shared by all WOFOST objects that have been
        initialized. This is implemented using the class '__dict__' attribute
        which is then copied into the object."""
        
        # Get the logger object
        logger = self.logger

        self.STATES  = copy.deepcopy(w60lib.states.__dict__)
        logger.debug("%s COMMON block STATES copied" % self.day)
        self.RATES   = copy.deepcopy(w60lib.rates.__dict__)
        logger.debug("%s COMMON block RATES copied" % self.day)
        self.CROOTD  = copy.deepcopy(w60lib.crootd.__dict__)
        logger.debug("%s COMMON block CROOTD copied" % self.day)
        self.CWATFD  = copy.deepcopy(w60lib.cwatfd.__dict__)
        logger.debug("%s COMMON block CWATFD copied" % self.day)
        self.CWATPP  = copy.deepcopy(w60lib.cwatpp.__dict__)
        logger.debug("%s COMMON block CWATPP copied" % self.day)
        self.SOILVAR = copy.deepcopy(w60lib.soilvar.__dict__)
        logger.debug("%s COMMON block SOILVAR copied" % self.day)
        self.CROPVAR = copy.deepcopy(w60lib.cropvar.__dict__)
        logger.debug("%s COMMON block CROPVAR copied" % self.day)
        self.SITEVAR = copy.deepcopy(w60lib.sitevar.__dict__)
        logger.debug("%s COMMON block SITEVAR copied" % self.day)

#------------------------------------------------------------------------------------------
    def _restore_COMMON_blocks(self):
        """This method copies the data stored in the WOFOST object back into the
        FORTRAN COMMON in py_w60lib blocks, so that the WOFOST model can
        continue from the point that it stopped."""
        
        # Get the logger object
        logger = self.logger

        # CROPSI COMMON Blocks RATES & STATES
        keys = self.STATES.keys()
        for key in keys:
            setattr(w60lib.states, key, self.STATES[key])
        logger.debug("%s COMMON block STATES restored" % self.day)

        keys = self.RATES.keys()
        for key in keys:
            setattr(w60lib.rates, key, self.RATES[key])
        logger.debug("%s COMMON block RATES restored" % self.day)

        # ROOTD COMMON Block
        keys = self.CROOTD.keys()
        for key in keys:
            setattr(w60lib.crootd, key, self.CROOTD[key])
        logger.debug("%s COMMON block CROOTD restored" % self.day)

        # WATFD COMMON Block
        keys = self.CWATFD.keys()
        for key in keys:
            setattr(w60lib.cwatfd, key, self.CWATFD[key])
        logger.debug("%s COMMON block CWATFD restored" % self.day)

        # WATPP COMMON Block
        keys = self.CWATPP.keys()
        for key in keys:
            setattr(w60lib.cwatpp, key, self.CWATPP[key])
        logger.debug("%s COMMON block CWATPP restored" % self.day)
        
        # COMMON BLOCKS holding soil, crop and site variables
        keys = self.SOILVAR.keys()
        for key in keys:
            setattr(w60lib.soilvar, key, self.SOILVAR[key])
        logger.debug("%s COMMON block SOILVAR restored" % self.day)

        keys = self.CROPVAR.keys()
        for key in keys:
            setattr(w60lib.cropvar, key, self.CROPVAR[key])
        logger.debug("%s COMMON block CROPVAR restored" % self.day)

        keys = self.SITEVAR.keys()
        for key in keys:
            setattr(w60lib.sitevar, key, self.SITEVAR[key])
        logger.debug("%s COMMON block SITEVAR restored" % self.day)

#-------------------------------------------------------------------------------
    def _CROPSI_integrate(self, dmeteo, SM, EVWMX, EVSMX, TRA, FR):
        """Calls w60lib.cropsi() with ITASK=3"""
        
        logger = self.logger
        ITASK = 3
        ISTATE_old = self.ISTATE

        r = w60lib.cropsi(ITASK, self.DOY, self.DELT, self.TIME,
                             self.IDEM, self.DOANTH,
                             self.IDHALT, self.CROP_TERMNL, self.ISTATE,
                             self.IWB, self.IOX,
                             dmeteo["LAT"], dmeteo["IRRAD"],
                             dmeteo["TMIN"], dmeteo["TMAX"],
                             dmeteo["E0"], dmeteo["ES0"],
                             dmeteo["ET0"], SM, EVWMX, EVSMX, TRA, FR)
        [self.IDEM, self.DOANTH, self.IDHALT, self.CROP_TERMNL,
         self.ISTATE, EVWMX, EVSMX, TRA, FR] = r
        if ISTATE_old == self.ISTATE:
            logger.debug("%s CROPSI: ISTATE=%i" % (self.day, self.ISTATE))
        else:
            # Crop has emerged.
            logger.debug("%s CROPSI: EMERGENCE ISTATE=3" % self.day)
        
        # Add crop transpiration (TRA) to self. This is only to ensure that the
        # get_attribute() method can reach the value of this variable in both
        # idle and active runtime state.
        self.TRA = TRA

        return (EVWMX, EVSMX, TRA, FR)

#-------------------------------------------------------------------------------
    def _CROPSI_calc_rates(self, dmeteo, SM, EVWMX, EVSMX, TRA, FR):
        """Calls w60lib.cropsi() with ITASK=2"""
        
        logger = self.logger
        ITASK = 2
        r = w60lib.cropsi(ITASK, self.DOY, self.DELT, self.TIME,
                             self.IDEM, self.DOANTH,
                             self.IDHALT, self.CROP_TERMNL, self.ISTATE,
                             self.IWB, self.IOX,
                             dmeteo["LAT"], dmeteo["IRRAD"],
                             dmeteo["TMIN"], dmeteo["TMAX"],
                             dmeteo["E0"], dmeteo["ES0"],
                             dmeteo["ET0"], SM, EVWMX, EVSMX, TRA, FR)
        [self.IDEM, self.DOANTH, self.IDHALT, self.CROP_TERMNL,
         self.ISTATE, EVWMX, EVSMX, TRA, FR] = r
        logger.debug("%s CROPSI: rate calculation." % self.day)

        # Add crop transpiration (TRA) to self. This is only to ensure that the
        # get_attribute() method can reach the value of this variable in both
        # idle and active runtime state.
        self.TRA = TRA

        return (EVWMX, EVSMX, TRA, FR)
#-------------------------------------------------------------------------------
    def _no_crop(self, dmeteo):
        """Routine fakes a crop, just copies some variables and returns them
        is only used when the waterbalance started before the crop emerges."""

        RD = w60lib.cropvar.rdi
        EVWMX = dmeteo["E0"]
        EVSMX = dmeteo["ES0"]
        TRA = 0.0
        
        # Update the values for TRA/RD in self
        self.TRA = TRA
        self.RD = RD
        
        return (EVWMX, EVSMX, TRA, RD)

#-------------------------------------------------------------------------------
    def _ROOTD_integrate(self, FR, RD):

        logger = self.logger
        ITASK = 3

        (d1, d2, RD) = w60lib.rootd(ITASK, self.DELT, self.IWB, FR, self.ZT,
                                        self.RDM, RD)
        logger.debug("%s ROOTD: RD=%s" % (self.day, RD))
        
        # Add rooting depth/RD to self. This is only to ensure that the
        # get_attribute() method can reach the value of this variable in both
        # idle and active runtime state.
        self.RD = RD

        return RD

#-------------------------------------------------------------------------------
    def _ROOTD_calc_rates(self, FR, RD):
        
        logger = self.logger
        ITASK = 2
        (d1, d2, RD) = w60lib.rootd(ITASK, self.DELT, self.IWB, FR, self.ZT,
                                        self.RDM, RD)
        logger.debug("%s ROOTD: RD=%s" % (self.day, RD))
        
        # Add rooting depth (RD) to self. This is only to ensure that the
        # get_attribute() method can reach the value of this variable in both
        # idle and active runtime state.
        self.RD = RD

        return RD

#-------------------------------------------------------------------------------
    def _WATPP_integrate(self, EVWMX, EVSMX, TRA, SM):
        
        logger = self.logger
        ITASK = 3
        SM = w60lib.watpp(ITASK, self.DELT, self.IAIRDU, EVWMX, EVSMX, TRA, SM)

        # Store value of SM as rootzone_moisture_content in self
#        self.rootzone_moisture_content = SM
        self.RMC = SM

        # Set profile soil moisture equal to root-zone soil moisture
#        self.profile_moisture_content = SM 
        self.PMC = SM

        logger.debug("%s WATPP: SM = %s" % (self.day, SM))

        return SM

#-------------------------------------------------------------------------------
    def _WATPP_calc_rates(self, EVWMX, EVSMX, TRA, SM):
    
        logger = self.logger
        ITASK = 2
        SM = w60lib.watpp(ITASK, self.DELT, self.IAIRDU, EVWMX, EVSMX, TRA, SM)

        # Store value of SM as rootzone_moisture_content in self
#        self.rootzone_moisture_content = SM
        self.RMC = SM

        # Set profile soil moisture equal to root-zone soil moisture
#        self.profile_moisture_content = SM 
        self.PMC = SM

        logger.debug("%s WATPP: SM = %s" % (self.day, SM))

        return SM

#-------------------------------------------------------------------------------
    def _WATFD_integrate(self, EVWMX, EVSMX, TRA, SM, RD, dmeteo):
        
        logger = self.logger
        ITASK = 3
        SM = w60lib.watfd(ITASK, self.DELT, self.IAIRDU, self.IDEM, self.IDHALT,
                          self.RDM, RD, EVWMX, EVSMX, TRA, SM, dmeteo["RAIN"])

        # Store value of SM as rootzone_moisture_content in self
#        self.rootzone_moisture_content = SM
        self.RMC = SM

        # Calc profile soil moisture
#        self.profile_moisture_content = float(w60lib.cwatfd.wwlow)/self.RDM
        self.PMC = float(w60lib.cwatfd.wwlow)/self.RDM

        logger.debug("%s WATFD: SM = %s" % (self.day, SM))

        return SM

#-------------------------------------------------------------------------------
    def _WATFD_calc_rates(self, EVWMX, EVSMX, TRA, SM, RD, dmeteo):
    
        logger = self.logger
        ITASK = 2
        SM = w60lib.watfd(ITASK, self.DELT, self.IAIRDU, self.IDEM, self.IDHALT,
                          self.RDM, RD, EVWMX, EVSMX, TRA, SM, dmeteo["RAIN"])

        # Store value of SM as rootzone_moisture_content in self
#        self.rootzone_moisture_content = SM
        self.RMC = SM

        # Set profile soil moisture equal to root-zone soil moisture
#        self.profile_moisture_content = float(w60lib.cwatfd.wwlow)/self.RDM
        self.PMC = float(w60lib.cwatfd.wwlow)/self.RDM

        logger.debug("%s WATFD: SM = %s" % (self.day, SM))

        return SM

    #---------------------------------------------------------------------------
    def _timer_update(self):
        "Progresses the timer"
        
        logger = self.logger
        
        r = self.timer()
        [current_date, self.TIME, self.DOY,
         self.YEAR, self.outflag, self.time_termnl] = r
        logstr = "Timer: update to %s" % current_date
        logger.debug(logstr)
        
        return current_date

    #---------------------------------------------------------------------------
    def _find_w60lib_variable(self, attr):
        "Find and return variable in w60lib COMMON blocks"
        
        # Search FORTRAN COMMON Blocks for locations of attributes.
        if hasattr(w60lib.states, attr):
            return getattr(w60lib.states, attr)
        elif hasattr(w60lib.rates, attr):
            return getattr(w60lib.rates, attr)
        elif hasattr(w60lib.crootd, attr):
            return getattr(w60lib.crootd, attr)
        elif hasattr(w60lib.cwatpp, attr) and self.IWB==0:
            return getattr(w60lib.cwatpp, attr)
        elif hasattr(w60lib.cwatfd, attr) and self.IWB==1:
            return getattr(w60lib.cwatfd, attr)
        else:
            return None
