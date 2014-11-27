** $Id: sweaf.for 1.2 1997/10/02 15:20:35 LEM release $
      REAL FUNCTION SWEAF (ET0, CGNR)

*     Chapter 20 in documentation WOFOST Version 4.1 (1988)

*     The fraction of easily available soil water between
*     field capacity and wilting point is a function of the
*     potential evapotranspiration rate (for a closed canopy)
*     in cm/day, ET0, and the crop group number, CGNR (from
*     1 (=drought-sensitive) to 5 (=drought-resistent)). The
*     function SWEAF describes this relationship given in tabular
*     form by Doorenbos & Kassam (1979) and by Van Keulen & Wolf
*     (1986; p.108, table 20).

*     Must be linked with object library TTUTIL.

*     Authors: D.M. Jansen and C.A. van Diepen, October 1986.

      IMPLICIT REAL(A-Z)
**
      SAVE

      DATA A /0.76/,B /1.5/
*     curve for CGNR 5, and other curves at fixed distance below it

      SWEAF = 1./(A+B*ET0) - (5.-CGNR)*0.10

*     correction for lower curves (CGNR less than 3)
      IF (CGNR.LT.3.) SWEAF = SWEAF + (ET0-0.6)/(CGNR*(CGNR+3.))
      SWEAF = LIMIT (0.10, 0.95, SWEAF)

      RETURN
      END
