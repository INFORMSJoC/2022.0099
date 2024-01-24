\ Equation counts
\     Total        E        G        L        N        X        C        B
\        31        0        0       31        0        0        0        0
\ 
\ Variable counts
\                  x        b        i      s1s      s2s       sc       si
\     Total     cont   binary  integer     sos1     sos2    scont     sint
\        60       45        0       15        0        0        0        0
\ 
\ Nonzero counts
\     Total    const       NL      DLL
\       120       90       30        0
\ 
Minimize
 obj: - .95 i1 - .92 i2 - 5.5e-2 i3 - .74 i4 - .27 i5 - .42 i6 - .55 i7 - .945 i8 - .42 i9 - .985 i10 
  - .3 i11 - .7 i12 - .665 i13 - .54 i14 - .7 i15 - .665 x16 - .18 x17 - .13 x18 - x19 - .17 x20 - 3.5e-2 x21 
  - .56 x22 - .88 x23 - .67 x24 - .19 x25 - .37 x26 - .46 x27 - .98 x28 - .155 x29 - .855 x30 + 0 x32 
  + 0 x33 + 0 x34 + 0 x35 + 0 x36 + 0 x37 + 0 x38 + 0 x39 + 0 x40 + 0 x41 + 0 x42 + 0 x43 + 0 x44 + 0 x45 
  + 0 x46 + 0 x47 + 0 x48 + 0 x49 + 0 x50 + 0 x51 + 0 x52 + 0 x53 + 0 x54 + 0 x55 + 0 x56 + 0 x57 + 0 x58 
  + 0 x59 + 0 x60 + 0 x61

Subject To
 e1: x32 + x33 + x34 + x35 + x36 + x37 + x38 + x39 + x40 + x41 + x42 + x43 + x44 + x45 + x46 + x47 + x48 
  + x49 + x50 + x51 + x52 + x53 + x54 + x55 + x56 + x57 + x58 + x59 + x60 + x61 <= 99.9999
 e3: - x32 + [ i1^2 ] <= 0
 e4: - x33 + [ i2^2 ] <= 0
 e5: - x34 + [ i3^2 ] <= 0
 e6: - x35 + [ i4^2 ] <= 0
 e7: - x36 + [ i5^2 ] <= 0
 e8: - x37 + [ i6^2 ] <= 0
 e9: - x38 + [ i7^2 ] <= 0
 e10: - x39 + [ i8^2 ] <= 0
 e11: - x40 + [ i9^2 ] <= 0
 e12: - x41 + [ i10^2 ] <= 0
 e13: - x42 + [ i11^2 ] <= 0
 e14: - x43 + [ i12^2 ] <= 0
 e15: - x44 + [ i13^2 ] <= 0
 e16: - x45 + [ i14^2 ] <= 0
 e17: - x46 + [ i15^2 ] <= 0
 e18: - x47 + [ x16^2 ] <= 0
 e19: - x48 + [ x17^2 ] <= 0
 e20: - x49 + [ x18^2 ] <= 0
 e21: - x50 + [ x19^2 ] <= 0
 e22: - x51 + [ x20^2 ] <= 0
 e23: - x52 + [ x21^2 ] <= 0
 e24: - x53 + [ x22^2 ] <= 0
 e25: - x54 + [ x23^2 ] <= 0
 e26: - x55 + [ x24^2 ] <= 0
 e27: - x56 + [ x25^2 ] <= 0
 e28: - x57 + [ x26^2 ] <= 0
 e29: - x58 + [ x27^2 ] <= 0
 e30: - x59 + [ x28^2 ] <= 0
 e31: - x60 + [ x29^2 ] <= 0
 e32: - x61 + [ x30^2 ] <= 0

Bounds
 i1 <= 5
 i2 <= 5
 i3 <= 5
 i4 <= 5
 i5 <= 5
 i6 <= 5
 i7 <= 5
 i8 <= 5
 i9 <= 5
 i10 <= 5
 i11 <= 5
 i12 <= 5
 i13 <= 5
 i14 <= 5
 i15 <= 5
 x16 <= 5
 x17 <= 5
 x18 <= 5
 x19 <= 5
 x20 <= 5
 x21 <= 5
 x22 <= 5
 x23 <= 5
 x24 <= 5
 x25 <= 5
 x26 <= 5
 x27 <= 5
 x28 <= 5
 x29 <= 5
 x30 <= 5
 x32 Free
 x33 Free
 x34 Free
 x35 Free
 x36 Free
 x37 Free
 x38 Free
 x39 Free
 x40 Free
 x41 Free
 x42 Free
 x43 Free
 x44 Free
 x45 Free
 x46 Free
 x47 Free
 x48 Free
 x49 Free
 x50 Free
 x51 Free
 x52 Free
 x53 Free
 x54 Free
 x55 Free
 x56 Free
 x57 Free
 x58 Free
 x59 Free
 x60 Free
 x61 Free

General
 i1 i2 i3 i4 i5 i6 i7 i8 i9 i10 i11 i12 i13 i14 i15

End
