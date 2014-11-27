
rm futil.so
f2py -c --fcompiler=gnu95 -m futil astro.for pmonteith.for totass.for penman.for  ./libttutil/libttutil.a ./libw60/libw60.a
