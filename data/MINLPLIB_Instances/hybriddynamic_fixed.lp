\ Equation counts
\     Total        E        G        L        N        X        C        B
\        79       59       10       10        0        0        0        0
\ 
\ Variable counts
\                  x        b        i      s1s      s2s       sc       si
\     Total     cont   binary  integer     sos1     sos2    scont     sint
\        71       61       10        0        0        0        0        0
\ 
\ Nonzero counts
\     Total    const       NL      DLL
\       189      178       11        0
\ 
Minimize
 obj: 0 x1 + 0 x2 + 0 x3 + 0 x4 + 0 x5 + 0 x6 + 0 x7 + 0 x8 + 0 x9 + 0 x10 + 0 b11 + 0 b12 + 0 b13 + 0 b14 
  + 0 b15 + 0 b16 + 0 b17 + 0 b18 + 0 b19 + 0 b20 + 0 x21 + 0 x22 + 0 x23 + 0 x24 + 0 x25 + 0 x26 + 0 x27 
  + 0 x28 + 0 x29 + 0 x30 + 0 x31 + 0 x32 + 0 x33 + 0 x34 + 0 x35 + 0 x36 + 0 x37 + 0 x38 + 0 x39 + 0 x40 
  + 0 x41 + 0 x42 + 0 x43 + 0 x44 + 0 x45 + 0 x46 + 0 x47 + 0 x48 + 0 x49 + 0 x50 + 0 x51 + 0 x52 + 0 x53 
  + 0 x54 + 0 x55 + 0 x56 + 0 x57 + 0 x58 + 0 x59 + 0 x60 + 0 x61 + 0 x62 + 0 x63 + 0 x64 + 0 x65 + 0 x66 
  + 0 x67 + 0 x68 + 0 x69 + 0 x70 - 3.33333333333334 x71 + [ .4 x51^2 + .4 x52^2 + .4 x53^2 + .4 x54^2 
  + .4 x55^2 + .4 x56^2 + .4 x57^2 + .4 x58^2 + .4 x59^2 + .4 x60^2 + 2 x71^2 ]/2 + objconstant

Subject To
 e2: - x31 + x41 = .2
 e3: - x32 + x42 = .2
 e4: - x33 + x43 = .2
 e5: - x34 + x44 = .2
 e6: - x35 + x45 = .2
 e7: - x36 + x46 = .2
 e8: - x37 + x47 = .2
 e9: - x38 + x48 = .2
 e10: - x39 + x49 = .2
 e11: - x40 + x50 = .2
 e12: - x21 + x51 - .2 x61 = 0
 e13: - x22 + x52 - .2 x62 = 0
 e14: - x23 + x53 - .2 x63 = 0
 e15: - x24 + x54 - .2 x64 = 0
 e16: - x25 + x55 - .2 x65 = 0
 e17: - x26 + x56 - .2 x66 = 0
 e18: - x27 + x57 - .2 x67 = 0
 e19: - x28 + x58 - .2 x68 = 0
 e20: - x29 + x59 - .2 x69 = 0
 e21: - x30 + x60 - .2 x70 = 0
 e22: x1 - 2 b11 = -1
 e23: x2 - 2 b12 = -1
 e24: x3 - 2 b13 = -1
 e25: x4 - 2 b14 = -1
 e26: x5 - 2 b15 = -1
 e27: x6 - 2 b16 = -1
 e28: x7 - 2 b17 = -1
 e29: x8 - 2 b18 = -1
 e30: x9 - 2 b19 = -1
 e31: x10 - 2 b20 = -1
 e32: 2 b11 - x51 <= 2
 e33: 2 b12 - x52 <= 2
 e34: 2 b13 - x53 <= 2
 e35: 2 b14 - x54 <= 2
 e36: 2 b15 - x55 <= 2
 e37: 2 b16 - x56 <= 2
 e38: 2 b17 - x57 <= 2
 e39: 2 b18 - x58 <= 2
 e40: 2 b19 - x59 <= 2
 e41: 2 b20 - x60 <= 2
 e42: 2 b11 - x51 >= 0
 e43: 2 b12 - x52 >= 0
 e44: 2 b13 - x53 >= 0
 e45: 2 b14 - x54 >= 0
 e46: 2 b15 - x55 >= 0
 e47: 2 b16 - x56 >= 0
 e48: 2 b17 - x57 >= 0
 e49: 2 b18 - x58 >= 0
 e50: 2 b19 - x59 >= 0
 e51: 2 b20 - x60 >= 0
 e52: - x31 + x32 = .2
 e53: - x32 + x33 = .2
 e54: - x33 + x34 = .2
 e55: - x34 + x35 = .2
 e56: - x35 + x36 = .2
 e57: - x36 + x37 = .2
 e58: - x37 + x38 = .2
 e59: - x38 + x39 = .2
 e60: - x39 + x40 = .2
 e61: - x21 + x22 - .2 x61 = 0
 e62: - x22 + x23 - .2 x62 = 0
 e63: - x23 + x24 - .2 x63 = 0
 e64: - x24 + x25 - .2 x64 = 0
 e65: - x25 + x26 - .2 x65 = 0
 e66: - x26 + x27 - .2 x66 = 0
 e67: - x27 + x28 - .2 x67 = 0
 e68: - x28 + x29 - .2 x68 = 0
 e69: - x29 + x30 - .2 x69 = 0
 e70: - x30 - .2 x70 + x71 = 0
 e71: x1 + x61 = 2
 e72: x2 + x62 = 2
 e73: x3 + x63 = 2
 e74: x4 + x64 = 2
 e75: x5 + x65 = 2
 e76: x6 + x66 = 2
 e77: x7 + x67 = 2
 e78: x8 + x68 = 2
 e79: x9 + x69 = 2
 e80: x10 + x70 = 2

Bounds
 -1 <= x1 <= 1
 -1 <= x2 <= 1
 -1 <= x3 <= 1
 -1 <= x4 <= 1
 -1 <= x5 <= 1
 -1 <= x6 <= 1
 -1 <= x7 <= 1
 -1 <= x8 <= 1
 -1 <= x9 <= 1
 -1 <= x10 <= 1
 x21 = -2
 x22 Free
 x23 Free
 x24 Free
 x25 Free
 x26 Free
 x27 Free
 x28 Free
 x29 Free
 x30 Free
 x31 = 0
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
 -2 <= x51 <= 2
 -2 <= x52 <= 2
 -2 <= x53 <= 2
 -2 <= x54 <= 2
 -2 <= x55 <= 2
 -2 <= x56 <= 2
 -2 <= x57 <= 2
 -2 <= x58 <= 2
 -2 <= x59 <= 2
 -2 <= x60 <= 2
 x61 Free
 x62 Free
 x63 Free
 x64 Free
 x65 Free
 x66 Free
 x67 Free
 x68 Free
 x69 Free
 x70 Free
 x71 Free
 objconstant = 2.7777777777777892

Binary
 b11 b12 b13 b14 b15 b16 b17 b18 b19 b20

End
