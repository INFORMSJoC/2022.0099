\ Equation counts
\     Total        E        G        L        N        X        C        B
\        21        0        0       21        0        0        0        0
\ 
\ Variable counts
\                  x        b        i      s1s      s2s       sc       si
\     Total     cont   binary  integer     sos1     sos2    scont     sint
\        40       30        0       10        0        0        0        0
\ 
\ Nonzero counts
\     Total    const       NL      DLL
\        80       60       20        0
\ 
Minimize
 obj: - .175 i1 - .39 i2 - .83 i3 - .805 i4 - 6e-2 i5 - .4 i6 - .52 i7 - .415 i8 - .655 i9 - .63 i10 
  - .29 x11 - .43 x12 - 1.5e-2 x13 - .985 x14 - .165 x15 - .105 x16 - .37 x17 - .2 x18 - .49 x19 - .34 x20 
  + 0 x22 + 0 x23 + 0 x24 + 0 x25 + 0 x26 + 0 x27 + 0 x28 + 0 x29 + 0 x30 + 0 x31 + 0 x32 + 0 x33 + 0 x34 
  + 0 x35 + 0 x36 + 0 x37 + 0 x38 + 0 x39 + 0 x40 + 0 x41

Subject To
 e1: x22 + x23 + x24 + x25 + x26 + x27 + x28 + x29 + x30 + x31 + x32 + x33 + x34 + x35 + x36 + x37 + x38 
  + x39 + x40 + x41 <= 99.9999
 e3: - x22 + [ i1^2 ] <= 0
 e4: - x23 + [ i2^2 ] <= 0
 e5: - x24 + [ i3^2 ] <= 0
 e6: - x25 + [ i4^2 ] <= 0
 e7: - x26 + [ i5^2 ] <= 0
 e8: - x27 + [ i6^2 ] <= 0
 e9: - x28 + [ i7^2 ] <= 0
 e10: - x29 + [ i8^2 ] <= 0
 e11: - x30 + [ i9^2 ] <= 0
 e12: - x31 + [ i10^2 ] <= 0
 e13: - x32 + [ x11^2 ] <= 0
 e14: - x33 + [ x12^2 ] <= 0
 e15: - x34 + [ x13^2 ] <= 0
 e16: - x35 + [ x14^2 ] <= 0
 e17: - x36 + [ x15^2 ] <= 0
 e18: - x37 + [ x16^2 ] <= 0
 e19: - x38 + [ x17^2 ] <= 0
 e20: - x39 + [ x18^2 ] <= 0
 e21: - x40 + [ x19^2 ] <= 0
 e22: - x41 + [ x20^2 ] <= 0

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
 x11 <= 5
 x12 <= 5
 x13 <= 5
 x14 <= 5
 x15 <= 5
 x16 <= 5
 x17 <= 5
 x18 <= 5
 x19 <= 5
 x20 <= 5
 x22 Free
 x23 Free
 x24 Free
 x25 Free
 x26 Free
 x27 Free
 x28 Free
 x29 Free
 x30 Free
 x31 Free
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

General
 i1 i2 i3 i4 i5 i6 i7 i8 i9 i10

End
