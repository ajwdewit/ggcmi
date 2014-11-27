rm *.o libw60.a
gfortran -fPIC -c *.for
ar rv libw60.a *.o

