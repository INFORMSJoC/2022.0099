\ Equation counts
\     Total        E        G        L        N        X        C        B
\         4        0        1        3        0        0        0        0
\ 
\ Variable counts
\                  x        b        i      s1s      s2s       sc       si
\     Total     cont   binary  integer     sos1     sos2    scont     sint
\         6        3        0        3        0        0        0        0
\ 
\ Nonzero counts
\     Total    const       NL      DLL
\        15       12        3        0
\ 
Minimize
 obj: 10 i1 - 4 i2 + 5 i3 + 2 x4 + 3 x5 - 500 x6 + [ 10 x4^2 + 10 x5^2 + 20 x6^2 ]/2

Subject To
 e1: x4 + x5 - x6 >= 0
 e2: - 5 i1 + x4 <= 0
 e3: - 10 i2 + x5 <= 0
 e4: - 30 i3 + x6 <= 0

Bounds
 i1 <= 1
 i2 <= 1
 i3 <= 1
 x4 <= 1e15
 x5 <= 1e15
 x6 <= 1e15

General
 i1 i2 i3

End
