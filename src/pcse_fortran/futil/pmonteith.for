C     IMPLICIT NONE

      REAL FUNCTION PMONTEITH (IDAY,LAT,ELEV,TMIN,TMAX,AVRAD,VAP,WIND2)
C     Calculates reference ET0 based on the Penman-Monteith model.

C     This routine calculates the potential evapotranspiration rate from
C     a reference crop canopy (ET0) in mm/d. For these calculations the
C     analysis by FAO is followed as laid down in the FAO publication
C     `Guidelines for computing crop water requirements - FAO Irrigation
C     and drainage paper 56
C     <http://www.fao.org/docrep/X0490E/x0490e00.htm#Contents>`_

C     Input variables::

C     DAY   - Day of the year (1 - 366)                  -
C     LAT   - Latitude of the site                       degrees
C     ELEV  - Elevation above sea level                  m
C     TMIN  - Minimum temperature                        C
C     TMAX  - Maximum temperature                        C
C     AVRAD - Daily shortwave radiation                  J m-2 d-1
C     VAP   - 24 hour average vapour pressure            hPa
C     WIND2 - 24 hour average windspeed at 2 meter       m/s

C     Returned output is:
C     ET0   - Penman-Monteith potential transpiration
C             rate from a crop canopy                    [mm/d]

C     FATAL ERROR CHECKS (execution terminated, message): none
C     WARNINGS: none
C     SUBROUTINES and FUNCTIONS called: ASTRO (directly) as well as
C         FATALERR and ILEN (indirectly)
C     FILE usage: none

C     Source:  Derived from file util.py of the Python implementation of
C              WOFOST dated April 2014 by Allard de WIt
C     Authors: Steven Hoek
C     Date:    July 2014

      INTEGER IDAY
      REAL LAT, ELEV, TMIN, TMAX, AVRAD, VAP, WIND2
      REAL PSYCON, REFCFC, CRES, LHVAP, STBC, G
      REAL T, TMPA, PATM, GAMMA, SVAP_TMPA, DELTA, SVAP_TMAX, SVAP_TMIN
      REAL SVAP, STB_TMAX, STB_TMIN, RNL_TMP, DAYL, ANGOT
      REAL DAYLP, SINLD, COSLD, DIFPP, ATMTR, DSINBE
      REAL CSKYRAD, RNL, RN, EA, MGAMMA, NUM, DENOM, ET0
      EXTERNAL ASTRO

Cf2py intent(in) IDAY
Cf2py intent(in) LAT
Cf2py intent(in) ELEV
Cf2py intent(in) TMIN
Cf2py intent(in) TMAX
Cf2py intent(in) AVRAD
Cf2py intent(in) VAP
Cf2py intent(in) WIND2
Cf2py intent(in) ANGSTA
Cf2py intent(in) ANGSTB
Cf2py intent(out) ET0

C     psychrometric instrument constant (kPa/Celsius)
      PARAMETER (PSYCON = 0.665)
C     latent heat of evaporation of water [J/kg == J/mm] and
      PARAMETER (LHVAP = 2.45E6)
C     Stefan Boltzmann constant (J/m2/d/K4, e.g multiplied by 24*60*60)
      PARAMETER (STBC = 4.903E-3)
C     albedo and surface resistance [sec/m] for the ref. crop canopy
      REFCFC = 0.23
      CRES = 70.0
C     Soil heat flux [J/m2/day] explicitly set to zero
      G = 0.0

C     mean daily temperature (Celsius)
      TMPA = (TMIN+TMAX) / 2.0

C     Vapour pressure to kPa
      VAP = HPA2KPA(VAP)

C     atmospheric pressure (kPa)
      T = C2K(TMPA)
      PATM = 101.3 * ((T - (0.0065*ELEV))/T)**5.26

C     psychrometric constant (kPa/Celsius)
      GAMMA = PSYCON * PATM * 1.0E-3

C     Derivative of SVAP with respect to mean temperature, i.e.
C     slope of the SVAP-temperature curve (kPa/Celsius);
      SVAP_TMPA = SATVAP(TMPA)
      DELTA = (4098. * SVAP_TMPA)/((TMPA + 237.3)**2)

C     Daily average saturated vapour pressure [kPa] from min/max
C     temperature
      SVAP_TMAX = SATVAP(TMAX)
      SVAP_TMIN = SATVAP(TMIN)
      SVAP = (SVAP_TMAX + SVAP_TMIN) / 2.

C     measured vapour pressure not to exceed saturated vapour pressure
      VAP = MIN(VAP, SVAP)

C     Longwave radiation according at Tmax, Tmin (J/m2/d)
C     and preliminary net outgoing long-wave radiation (J/m2/d)
      STB_TMAX = STBC * (C2K(TMAX)**4)
      STB_TMIN = STBC * (C2K(TMIN)**4)
      RNL_TMP = ((STB_TMAX + STB_TMIN) / 2.) * (0.34 - 0.14 * SQRT(VAP))

C     Clear Sky radiation [J/m2/day] from Angot TOA radiation
C     the latter is found through a call to ASTRO()
      CALL ASTRO(IDAY, LAT, AVRAD, DAYL, DAYLP, SINLD, COSLD, DIFPP,
     1    ATMTR, DSINBE)
      IF (DAYL .GT. 0.0) THEN
          ANGOT = AVRAD / ATMTR
      ELSE
          ANGOT = 0.0
      ENDIF
      CSKYRAD = (0.75 + (2E-05 * ELEV)) * ANGOT

      IF (CSKYRAD .GT. 0.0) THEN
C         Final net outgoing longwave radiation [J/m2/day]
          RNL = RNL_TMP * (1.35 * (AVRAD/CSKYRAD) - 0.35)

C         radiative evaporation equivalent for the reference surface
C         [mm/day]
          RN = ((1-REFCFC) * AVRAD - RNL)/LHVAP

C         aerodynamic evaporation equivalent [mm/day]
          EA = ((900./(TMPA + 273)) * WIND2 * (SVAP - VAP))

C         Modified psychometric constant (gamma*)[kPa/C]
          MGAMMA = GAMMA * (1. + (CRES/208.*WIND2))

C         Reference ET in mm/day
          NUM = (DELTA * (RN-G)) + (GAMMA * EA)
          DENOM = DELTA + MGAMMA
          ET0 = MAX(0.0, NUM / DENOM)
      ELSE
          ET0 = 0.0
      ENDIF

      PMONTEITH = ET0

      RETURN

      END


      REAL FUNCTION C2K (T)
C     Degrees Celsius to degrees Kelvin
      REAL T
      C2K = T + 273.16
      RETURN
      END


      REAL FUNCTION HPA2KPA (P)
C     Hectopascal to kilopascal
      REAL P
      HPA2KPA = P/10.
      RETURN
      END


      REAL FUNCTION SATVAP (T)
C     Calculates saturated vapour pressure
      REAL T
      SATVAP = 0.6108 * EXP((17.27 * T) / (237.3 + T))
      RETURN
      END

