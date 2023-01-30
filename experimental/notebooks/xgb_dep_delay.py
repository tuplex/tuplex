import math

def xgb_tree(x, num_booster):
    if num_booster == 0:
        state = 0
        if state == 0:
            state = (1 if x['7']<88 else 2)
            if state == 1:
                state = (3 if x['7']<27 else 4)
                if state == 3:
                    state = (7 if x['7']<8 else 8)
                    if state == 7:
                        return -22.234478
                    if state == 8:
                        return -8.52761555
                if state == 4:
                    state = (9 if x['7']<53 else 10)
                    if state == 9:
                        return 8.87654972
                    if state == 10:
                        return 32.7460327
            if state == 2:
                state = (5 if x['7']<320 else 6)
                if state == 5:
                    state = (11 if x['7']<169 else 12)
                    if state == 11:
                        return 73.5685959
                    if state == 12:
                        return 153.027878
                if state == 6:
                    state = (13 if x['8']<330 else 14)
                    if state == 13:
                        return 240.612183
                    if state == 14:
                        return 396.957611
    elif num_booster == 1:
        state = 0
        if state == 0:
            state = (1 if x['7']<122 else 2)
            if state == 1:
                state = (3 if x['7']<18 else 4)
                if state == 3:
                    state = (7 if x['7']<-2 else 8)
                    if state == 7:
                        return -7.76837921
                    if state == 8:
                        return -3.17198873
                if state == 4:
                    state = (9 if x['7']<40 else 10)
                    if state == 9:
                        return 0.435146004
                    if state == 10:
                        return 6.04980612
            if state == 2:
                state = (5 if x['7']<255 else 6)
                if state == 5:
                    state = (11 if x['7']<169 else 12)
                    if state == 11:
                        return 33.5415764
                    if state == 12:
                        return 17.7954502
                if state == 6:
                    state = (13 if x['4']<7 else 14)
                    if state == 13:
                        return 104.718117
                    if state == 14:
                        return 64.3530884
    elif num_booster == 2:
        state = 0
        if state == 0:
            state = (1 if x['7']<221 else 2)
            if state == 1:
                state = (3 if x['7']<68 else 4)
                if state == 3:
                    state = (7 if x['7']<53 else 8)
                    if state == 7:
                        return -0.435342222
                    if state == 8:
                        return -4.62820196
                if state == 4:
                    state = (9 if x['7']<88 else 10)
                    if state == 9:
                        return 9.11049843
                    if state == 10:
                        return -0.133604482
            if state == 2:
                state = (5 if x['10']<18 else 6)
                if state == 5:
                    state = (11 if x['7']<320 else 12)
                    if state == 11:
                        return 25.4440517
                    if state == 12:
                        return -7.08335114
                if state == 6:
                    state = (13 if x['8']<330 else 14)
                    if state == 13:
                        return 24.1214027
                    if state == 14:
                        return 96.4631271
    elif num_booster == 3:
        state = 0
        if state == 0:
            state = (1 if x['7']<1542 else 2)
            if state == 1:
                state = (3 if x['7']<134 else 4)
                if state == 3:
                    state = (5 if x['7']<-6 else 6)
                    if state == 5:
                        return -3.91016841
                    if state == 6:
                        return -0.0131124975
                if state == 4:
                    state = (7 if x['7']<169 else 8)
                    if state == 7:
                        return 12.6757193
                    if state == 8:
                        return -0.781581879
            if state == 2:
                return 408.364929
    elif num_booster == 4:
        state = 0
        if state == 0:
            state = (1 if x['7']<1542 else 2)
            if state == 1:
                state = (3 if x['7']<198 else 4)
                if state == 3:
                    state = (5 if x['7']<169 else 6)
                    if state == 5:
                        return 0.00308877998
                    if state == 6:
                        return -11.6606894
                if state == 4:
                    state = (7 if x['7']<255 else 8)
                    if state == 7:
                        return 10.1676092
                    if state == 8:
                        return 1.12481487
            if state == 2:
                return 245.018951
    elif num_booster == 5:
        state = 0
        if state == 0:
            state = (1 if x['7']<1 else 2)
            if state == 1:
                state = (3 if x['7']<-11 else 4)
                if state == 3:
                    state = (7 if x['5']<48 else 8)
                    if state == 7:
                        return -4.74267292
                    if state == 8:
                        return -6.44514561
                if state == 4:
                    state = (9 if x['7']<-2 else 10)
                    if state == 9:
                        return -0.309981048
                    if state == 10:
                        return -1.0565455
            if state == 2:
                state = (5 if x['7']<8 else 6)
                if state == 5:
                    state = (11 if x['7']<4 else 12)
                    if state == 11:
                        return 1.02156162
                    if state == 12:
                        return 3.84775519
                if state == 6:
                    state = (13 if x['7']<13 else 14)
                    if state == 13:
                        return -3.53248763
                    if state == 14:
                        return 0.500382125
    elif num_booster == 6:
        state = 0
        if state == 0:
            state = (1 if x['7']<1542 else 2)
            if state == 1:
                state = (3 if x['7']<88 else 4)
                if state == 3:
                    state = (5 if x['7']<80 else 6)
                    if state == 5:
                        return 0.0569512546
                    if state == 6:
                        return 6.70296717
                if state == 4:
                    state = (7 if x['7']<100 else 8)
                    if state == 7:
                        return -10.6071081
                    if state == 8:
                        return 0.440334529
            if state == 2:
                return 146.811234
    elif num_booster == 7:
        state = 0
        if state == 0:
            state = (1 if x['7']<27 else 2)
            if state == 1:
                state = (3 if x['7']<22 else 4)
                if state == 3:
                    state = (7 if x['7']<16 else 8)
                    if state == 7:
                        return -0.165242419
                    if state == 8:
                        return 0.930975854
                if state == 4:
                    state = (9 if x['7']<24 else 10)
                    if state == 9:
                        return 3.18452692
                    if state == 10:
                        return 5.15886402
            if state == 2:
                state = (5 if x['7']<33 else 6)
                if state == 5:
                    state = (11 if x['7']<30 else 12)
                    if state == 11:
                        return -6.37245178
                    if state == 12:
                        return -4.05416059
                if state == 6:
                    state = (13 if x['7']<53 else 14)
                    if state == 13:
                        return 1.7913624
                    if state == 14:
                        return -0.612160563
    elif num_booster == 8:
        state = 0
        if state == 0:
            state = (1 if x['7']<149 else 2)
            if state == 1:
                state = (3 if x['7']<122 else 4)
                if state == 3:
                    state = (7 if x['7']<112 else 8)
                    if state == 7:
                        return -0.0532079414
                    if state == 8:
                        return 8.00393486
                if state == 4:
                    state = (9 if x['7']<141 else 10)
                    if state == 9:
                        return -6.1543684
                    if state == 10:
                        return -1.55069721
            if state == 2:
                state = (5 if x['7']<169 else 6)
                if state == 5:
                    state = (11 if x['7']<159 else 12)
                    if state == 11:
                        return 5.39246559
                    if state == 12:
                        return 13.4921207
                if state == 6:
                    state = (13 if x['7']<181 else 14)
                    if state == 13:
                        return -8.57211304
                    if state == 14:
                        return 1.36054742
    elif num_booster == 9:
        state = 0
        if state == 0:
            state = (1 if x['7']<1542 else 2)
            if state == 1:
                state = (3 if x['7']<44 else 4)
                if state == 3:
                    state = (5 if x['7']<27 else 6)
                    if state == 5:
                        return 0.0872356817
                    if state == 6:
                        return -1.29666293
                if state == 4:
                    state = (7 if x['7']<53 else 8)
                    if state == 7:
                        return 3.63467216
                    if state == 8:
                        return -0.35446161
            if state == 2:
                return 87.7873535
    elif num_booster == 10:
        state = 0
        if state == 0:
            state = (1 if x['7']<-4 else 2)
            if state == 1:
                state = (3 if x['7']<-9 else 4)
                if state == 3:
                    state = (7 if x['7']<-11 else 8)
                    if state == 7:
                        return -0.933254898
                    if state == 8:
                        return -1.58973491
                if state == 4:
                    state = (9 if x['7']<-6 else 10)
                    if state == 9:
                        return 0.382150859
                    if state == 10:
                        return -0.803076267
            if state == 2:
                state = (5 if x['7']<-2 else 6)
                if state == 5:
                    state = (11 if x['7']<-3 else 12)
                    if state == 11:
                        return 0.269652545
                    if state == 12:
                        return 1.06954372
                if state == 6:
                    state = (13 if x['7']<-1 else 14)
                    if state == 13:
                        return -1.20999777
                    if state == 14:
                        return 0.0676893592
    elif num_booster == 11:
        state = 0
        if state == 0:
            state = (1 if x['7']<1542 else 2)
            if state == 1:
                state = (3 if x['7']<61 else 4)
                if state == 3:
                    state = (5 if x['7']<53 else 6)
                    if state == 5:
                        return 0.0229934715
                    if state == 6:
                        return -3.00220299
                if state == 4:
                    state = (7 if x['7']<68 else 8)
                    if state == 7:
                        return 2.93856955
                    if state == 8:
                        return 0.0419228598
            if state == 2:
                return 52.6453133
    elif num_booster == 12:
        state = 0
        if state == 0:
            state = (1 if x['8']<264 else 2)
            if state == 1:
                state = (3 if x['7']<255 else 4)
                if state == 3:
                    state = (7 if x['8']<228 else 8)
                    if state == 7:
                        return -0.00421095034
                    if state == 8:
                        return 3.8545773
                if state == 4:
                    state = (9 if x['4']<7 else 10)
                    if state == 9:
                        return -33.3859558
                    if state == 10:
                        return -3.51567316
            if state == 2:
                state = (5 if x['9']<7 else 6)
                if state == 5:
                    state = (11 if x['6']<6 else 12)
                    if state == 11:
                        return 149.200699
                    if state == 12:
                        return -9.00426006
                if state == 6:
                    state = (13 if x['6']<41 else 14)
                    if state == 13:
                        return -5.34377813
                    if state == 14:
                        return 14.8290377
    elif num_booster == 13:
        state = 0
        if state == 0:
            state = (1 if x['7']<320 else 2)
            if state == 1:
                state = (3 if x['8']<264 else 4)
                if state == 3:
                    state = (7 if x['7']<8 else 8)
                    if state == 7:
                        return 0.126751423
                    if state == 8:
                        return -0.120401278
                if state == 4:
                    state = (9 if x['4']<7 else 10)
                    if state == 9:
                        return -15.2712574
                    if state == 10:
                        return 12.847455
            if state == 2:
                state = (5 if x['4']<7 else 6)
                if state == 5:
                    state = (11 if x['3']<6 else 12)
                    if state == 11:
                        return -8.83711147
                    if state == 12:
                        return 68.9564819
                if state == 6:
                    state = (13 if x['2']<28 else 14)
                    if state == 13:
                        return -6.52513266
                    if state == 14:
                        return -61.1048775
    elif num_booster == 14:
        state = 0
        if state == 0:
            state = (1 if x['7']<20 else 2)
            if state == 1:
                state = (3 if x['7']<8 else 4)
                if state == 3:
                    state = (7 if x['7']<6 else 8)
                    if state == 7:
                        return -0.0764308572
                    if state == 8:
                        return 1.49307752
                if state == 4:
                    state = (9 if x['7']<10 else 10)
                    if state == 9:
                        return -1.77331197
                    if state == 10:
                        return -0.173668802
            if state == 2:
                state = (5 if x['7']<27 else 6)
                if state == 5:
                    state = (11 if x['7']<26 else 12)
                    if state == 11:
                        return 0.708079875
                    if state == 12:
                        return 1.85952437
                if state == 6:
                    state = (13 if x['7']<47 else 14)
                    if state == 13:
                        return -0.371136993
                    if state == 14:
                        return 0.279658765
    elif num_booster == 15:
        state = 0
        if state == 0:
            state = (1 if x['7']<1542 else 2)
            if state == 1:
                state = (3 if x['7']<0 else 4)
                if state == 3:
                    state = (5 if x['7']<-2 else 6)
                    if state == 5:
                        return -0.0597357713
                    if state == 6:
                        return -0.407318592
                if state == 4:
                    state = (7 if x['7']<88 else 8)
                    if state == 7:
                        return 0.103607453
                    if state == 8:
                        return -0.314463407
            if state == 2:
                return 37.1477051
    elif num_booster == 16:
        state = 0
        if state == 0:
            state = (1 if x['7']<108 else 2)
            if state == 1:
                state = (3 if x['7']<88 else 4)
                if state == 3:
                    state = (7 if x['7']<75 else 8)
                    if state == 7:
                        return -0.0401612967
                    if state == 8:
                        return 1.91719532
                if state == 4:
                    state = (9 if x['7']<94 else 10)
                    if state == 9:
                        return -3.50827146
                    if state == 10:
                        return -0.47565636
            if state == 2:
                state = (5 if x['10']<1 else 6)
                if state == 5:
                    state = (11 if x['5']<29 else 12)
                    if state == 11:
                        return 0.346280932
                    if state == 12:
                        return 170.4216
                if state == 6:
                    state = (13 if x['10']<6 else 14)
                    if state == 13:
                        return -13.443614
                    if state == 14:
                        return 0.588520706
    elif num_booster == 17:
        state = 0
        if state == 0:
            state = (1 if x['7']<53 else 2)
            if state == 1:
                state = (3 if x['7']<49 else 4)
                if state == 3:
                    state = (7 if x['7']<44 else 8)
                    if state == 7:
                        return 0.0242519062
                    if state == 8:
                        return -0.697689533
                if state == 4:
                    state = (9 if x['7']<51 else 10)
                    if state == 9:
                        return 1.88204706
                    if state == 10:
                        return 3.43736315
            if state == 2:
                state = (5 if x['7']<56 else 6)
                if state == 5:
                    state = (11 if x['7']<55 else 12)
                    if state == 11:
                        return -3.19825625
                    if state == 12:
                        return -1.97662795
                if state == 6:
                    state = (13 if x['12']<3 else 14)
                    if state == 13:
                        return -0.661755264
                    if state == 14:
                        return 0.115139201
    elif num_booster == 18:
        state = 0
        if state == 0:
            state = (1 if x['7']<1542 else 2)
            if state == 1:
                state = (3 if x['7']<198 else 4)
                if state == 3:
                    state = (5 if x['7']<149 else 6)
                    if state == 5:
                        return -0.00711441645
                    if state == 6:
                        return 0.977456033
                if state == 4:
                    state = (7 if x['10']<14 else 8)
                    if state == 7:
                        return -4.01392221
                    if state == 8:
                        return 2.10374427
            if state == 2:
                return 22.0071774
    elif num_booster == 19:
        state = 0
        if state == 0:
            state = (1 if x['7']<68 else 2)
            if state == 1:
                state = (3 if x['7']<66 else 4)
                if state == 3:
                    state = (7 if x['7']<-8 else 8)
                    if state == 7:
                        return -0.479818076
                    if state == 8:
                        return 0.0289163142
                if state == 4:
                    state = (9 if x['12']<3 else 10)
                    if state == 9:
                        return 3.01902366
                    if state == 10:
                        return 2.39417768
            if state == 2:
                state = (5 if x['7']<73 else 6)
                if state == 5:
                    state = (11 if x['7']<71 else 12)
                    if state == 11:
                        return -4.04242611
                    if state == 12:
                        return -2.10209703
                if state == 6:
                    state = (13 if x['10']<9 else 14)
                    if state == 13:
                        return 1.91581368
                    if state == 14:
                        return -0.130044371
    elif num_booster == 20:
        state = 0
        if state == 0:
            state = (1 if x['7']<122 else 2)
            if state == 1:
                state = (3 if x['7']<117 else 4)
                if state == 3:
                    state = (7 if x['7']<108 else 8)
                    if state == 7:
                        return -0.00155353092
                    if state == 8:
                        return 0.926984966
                if state == 4:
                    state = (9 if x['10']<9 else 10)
                    if state == 9:
                        return 2.51264095
                    if state == 10:
                        return 3.76613045
            if state == 2:
                state = (5 if x['4']<4 else 6)
                if state == 5:
                    state = (11 if x['7']<320 else 12)
                    if state == 11:
                        return -0.606956303
                    if state == 12:
                        return -37.4565735
                if state == 6:
                    state = (13 if x['4']<6 else 14)
                    if state == 13:
                        return 21.8928509
                    if state == 14:
                        return -0.354518026
    elif num_booster == 21:
        state = 0
        if state == 0:
            state = (1 if x['8']<330 else 2)
            if state == 1:
                state = (3 if x['7']<320 else 4)
                if state == 3:
                    state = (7 if x['7']<100 else 8)
                    if state == 7:
                        return 0.0136747807
                    if state == 8:
                        return -0.301770657
                if state == 4:
                    state = (9 if x['4']<7 else 10)
                    if state == 9:
                        return -37.2472038
                    if state == 10:
                        return 12.727438
            if state == 2:
                state = (5 if x['12']<3 else 6)
                if state == 5:
                    state = (11 if x['9']<6 else 12)
                    if state == 11:
                        return 123.375343
                    if state == 12:
                        return -21.6639843
                if state == 6:
                    state = (13 if x['4']<7 else 14)
                    if state == 13:
                        return 31.9835796
                    if state == 14:
                        return -5.51230621
    elif num_booster == 22:
        state = 0
        if state == 0:
            state = (1 if x['8']<190 else 2)
            if state == 1:
                state = (3 if x['7']<169 else 4)
                if state == 3:
                    state = (7 if x['7']<159 else 8)
                    if state == 7:
                        return -0.00960012153
                    if state == 8:
                        return 2.32434654
                if state == 4:
                    state = (9 if x['7']<181 else 10)
                    if state == 9:
                        return -2.18376732
                    if state == 10:
                        return -0.149727151
            if state == 2:
                state = (5 if x['10']<9 else 6)
                if state == 5:
                    state = (11 if x['9']<11 else 12)
                    if state == 11:
                        return 1.80213225
                    if state == 12:
                        return 21.8483829
                if state == 6:
                    state = (13 if x['9']<17 else 14)
                    if state == 13:
                        return -4.25600576
                    if state == 14:
                        return 2.13456583
    elif num_booster == 23:
        state = 0
        if state == 0:
            state = (1 if x['4']<12 else 2)
            if state == 1:
                state = (3 if x['7']<255 else 4)
                if state == 3:
                    state = (7 if x['8']<264 else 8)
                    if state == 7:
                        return -0.0197087508
                    if state == 8:
                        return 10.0714893
                if state == 4:
                    state = (9 if x['2']<31 else 10)
                    if state == 9:
                        return -8.90804195
                    if state == 10:
                        return 50.6778069
            if state == 2:
                state = (5 if x['8']<264 else 6)
                if state == 5:
                    state = (11 if x['7']<8 else 12)
                    if state == 11:
                        return 0.0687244385
                    if state == 12:
                        return -0.0437416881
                if state == 6:
                    state = (13 if x['5']<3 else 14)
                    if state == 13:
                        return 84.9452438
                    if state == 14:
                        return 3.21870232
    elif num_booster == 24:
        state = 0
        if state == 0:
            state = (1 if x['7']<37 else 2)
            if state == 1:
                state = (3 if x['7']<33 else 4)
                if state == 3:
                    state = (7 if x['7']<31 else 8)
                    if state == 7:
                        return -0.0118905315
                    if state == 8:
                        return 1.04750872
                if state == 4:
                    state = (9 if x['7']<35 else 10)
                    if state == 9:
                        return -2.04604721
                    if state == 10:
                        return -0.499982178
            if state == 2:
                state = (5 if x['7']<40 else 6)
                if state == 5:
                    state = (11 if x['7']<38 else 12)
                    if state == 11:
                        return 0.771539867
                    if state == 12:
                        return 1.97350085
                if state == 6:
                    state = (13 if x['7']<41 else 14)
                    if state == 13:
                        return -1.31406546
                    if state == 14:
                        return 0.0609284043
    elif num_booster == 25:
        state = 0
        if state == 0:
            state = (1 if x['7']<100 else 2)
            if state == 1:
                state = (3 if x['7']<96 else 4)
                if state == 3:
                    state = (7 if x['7']<88 else 8)
                    if state == 7:
                        return 0.00528754666
                    if state == 8:
                        return -0.47355485
                if state == 4:
                    state = (9 if x['10']<9 else 10)
                    if state == 9:
                        return 1.14573646
                    if state == 10:
                        return 2.64098167
            if state == 2:
                state = (5 if x['7']<104 else 6)
                if state == 5:
                    state = (11 if x['10']<9 else 12)
                    if state == 11:
                        return -4.59556007
                    if state == 12:
                        return -2.78121138
                if state == 6:
                    state = (13 if x['2']<3 else 14)
                    if state == 13:
                        return 2.1809051
                    if state == 14:
                        return -0.164109334
    elif num_booster == 26:
        state = 0
        if state == 0:
            state = (1 if x['9']<23 else 2)
            if state == 1:
                state = (3 if x['8']<330 else 4)
                if state == 3:
                    state = (7 if x['7']<82 else 8)
                    if state == 7:
                        return -0.0110223358
                    if state == 8:
                        return 0.127966225
                if state == 4:
                    state = (9 if x['9']<17 else 10)
                    if state == 9:
                        return -7.82301044
                    if state == 10:
                        return 20.2157879
            if state == 2:
                state = (5 if x['8']<330 else 6)
                if state == 5:
                    state = (11 if x['7']<320 else 12)
                    if state == 11:
                        return -0.0294818673
                    if state == 12:
                        return -17.582056
                if state == 6:
                    state = (13 if x['2']<26 else 14)
                    if state == 13:
                        return -93.0350571
                    if state == 14:
                        return 29.9375896
    elif num_booster == 27:
        state = 0
        if state == 0:
            state = (1 if x['7']<112 else 2)
            if state == 1:
                state = (3 if x['7']<108 else 4)
                if state == 3:
                    state = (7 if x['7']<85 else 8)
                    if state == 7:
                        return -0.00133403996
                    if state == 8:
                        return 0.266634703
                if state == 4:
                    state = (9 if x['2']<3 else 10)
                    if state == 9:
                        return -0.00103043776
                    if state == 10:
                        return 1.99455798
            if state == 2:
                state = (5 if x['9']<13 else 6)
                if state == 5:
                    state = (11 if x['8']<330 else 12)
                    if state == 11:
                        return 0.149374425
                    if state == 12:
                        return 11.1670885
                if state == 6:
                    state = (13 if x['7']<320 else 14)
                    if state == 13:
                        return -0.325452179
                    if state == 14:
                        return -6.4528017
    elif num_booster == 28:
        state = 0
        if state == 0:
            state = (1 if x['9']<6 else 2)
            if state == 1:
                state = (3 if x['7']<320 else 4)
                if state == 3:
                    state = (7 if x['8']<264 else 8)
                    if state == 7:
                        return 0.0284509212
                    if state == 8:
                        return -11.7377605
                if state == 4:
                    state = (9 if x['6']<40 else 10)
                    if state == 9:
                        return 3.33663559
                    if state == 10:
                        return -141.834991
            if state == 2:
                state = (5 if x['7']<320 else 6)
                if state == 5:
                    state = (11 if x['8']<330 else 12)
                    if state == 11:
                        return 0.000186281919
                    if state == 12:
                        return -3.40016437
                if state == 6:
                    state = (13 if x['11']<2 else 14)
                    if state == 13:
                        return 22.6915207
                    if state == 14:
                        return -2.50376487
    elif num_booster == 29:
        state = 0
        if state == 0:
            state = (1 if x['7']<149 else 2)
            if state == 1:
                state = (3 if x['7']<134 else 4)
                if state == 3:
                    state = (7 if x['7']<128 else 8)
                    if state == 7:
                        return -0.00778877083
                    if state == 8:
                        return 2.58357501
                if state == 4:
                    state = (9 if x['7']<141 else 10)
                    if state == 9:
                        return -2.24837232
                    if state == 10:
                        return 0.101007044
            if state == 2:
                state = (5 if x['4']<34 else 6)
                if state == 5:
                    state = (11 if x['1']<9 else 12)
                    if state == 11:
                        return -0.868742585
                    if state == 12:
                        return 1.98868883
                if state == 6:
                    state = (13 if x['8']<330 else 14)
                    if state == 13:
                        return 0.401372403
                    if state == 14:
                        return 34.9745903
    elif num_booster == 30:
        state = 0
        if state == 0:
            state = (1 if x['8']<330 else 2)
            if state == 1:
                state = (3 if x['7']<149 else 4)
                if state == 3:
                    state = (7 if x['7']<122 else 8)
                    if state == 7:
                        return 0.00868295971
                    if state == 8:
                        return -0.619407237
                if state == 4:
                    state = (9 if x['10']<14 else 10)
                    if state == 9:
                        return 1.68509924
                    if state == 10:
                        return -0.37871623
            if state == 2:
                state = (5 if x['1']<11 else 6)
                if state == 5:
                    state = (11 if x['0']<4 else 12)
                    if state == 11:
                        return -3.93762851
                    if state == 12:
                        return 60.5955086
                if state == 6:
                    state = (13 if x['9']<19 else 14)
                    if state == 13:
                        return -10.0460443
                    if state == 14:
                        return -92.3387451
    elif num_booster == 31:
        state = 0
        if state == 0:
            state = (1 if x['10']<13 else 2)
            if state == 1:
                state = (3 if x['8']<330 else 4)
                if state == 3:
                    state = (7 if x['8']<264 else 8)
                    if state == 7:
                        return -0.0240754057
                    if state == 8:
                        return 3.30767798
                if state == 4:
                    state = (9 if x['3']<7 else 10)
                    if state == 9:
                        return -2.16438389
                    if state == 10:
                        return -40.1484489
            if state == 2:
                state = (5 if x['8']<330 else 6)
                if state == 5:
                    state = (11 if x['7']<198 else 12)
                    if state == 11:
                        return 0.0281664133
                    if state == 12:
                        return -1.01333058
                if state == 6:
                    state = (13 if x['5']<4 else 14)
                    if state == 13:
                        return 136.765656
                    if state == 14:
                        return 4.3122468
    elif num_booster == 32:
        state = 0
        if state == 0:
            state = (1 if x['7']<128 else 2)
            if state == 1:
                state = (3 if x['7']<122 else 4)
                if state == 3:
                    state = (7 if x['7']<117 else 8)
                    if state == 7:
                        return -0.00707255071
                    if state == 8:
                        return 1.05498445
                if state == 4:
                    state = (9 if x['2']<3 else 10)
                    if state == 9:
                        return -3.67327547
                    if state == 10:
                        return -1.50317383
            if state == 2:
                state = (5 if x['10']<1 else 6)
                if state == 5:
                    state = (11 if x['0']<3 else 12)
                    if state == 11:
                        return 2.13099194
                    if state == 12:
                        return 118.735451
                if state == 6:
                    state = (13 if x['1']<3 else 14)
                    if state == 13:
                        return 2.61091924
                    if state == 14:
                        return -0.198297605
    elif num_booster == 33:
        state = 0
        if state == 0:
            state = (1 if x['7']<8 else 2)
            if state == 1:
                state = (3 if x['7']<7 else 4)
                if state == 3:
                    state = (7 if x['7']<-5 else 8)
                    if state == 7:
                        return -0.12460202
                    if state == 8:
                        return 0.0440448187
                if state == 4:
                    state = (9 if x['4']<12 else 10)
                    if state == 9:
                        return 0.642407596
                    if state == 10:
                        return 0.56987375
            if state == 2:
                state = (5 if x['7']<11 else 6)
                if state == 5:
                    state = (11 if x['7']<9 else 12)
                    if state == 11:
                        return -0.794782579
                    if state == 12:
                        return -0.281832188
                if state == 6:
                    state = (13 if x['7']<13 else 14)
                    if state == 13:
                        return 0.728480279
                    if state == 14:
                        return -0.0325433202
    elif num_booster == 34:
        state = 0
        if state == 0:
            state = (1 if x['7']<-6 else 2)
            if state == 1:
                state = (3 if x['7']<-7 else 4)
                if state == 3:
                    state = (7 if x['7']<-9 else 8)
                    if state == 7:
                        return 0.189978227
                    if state == 8:
                        return -0.19839713
                if state == 4:
                    state = (9 if x['4']<12 else 10)
                    if state == 9:
                        return 0.804802835
                    if state == 10:
                        return 0.732121408
            if state == 2:
                state = (5 if x['7']<-5 else 6)
                if state == 5:
                    state = (11 if x['4']<12 else 12)
                    if state == 11:
                        return -0.563613117
                    if state == 12:
                        return -0.637689173
                if state == 6:
                    state = (13 if x['7']<15 else 14)
                    if state == 13:
                        return -0.0363649093
                    if state == 14:
                        return 0.0484437607
    elif num_booster == 35:
        state = 0
        if state == 0:
            state = (1 if x['7']<4 else 2)
            if state == 1:
                state = (3 if x['7']<3 else 4)
                if state == 3:
                    state = (7 if x['7']<1 else 8)
                    if state == 7:
                        return 0.0529849902
                    if state == 8:
                        return -0.327123433
                if state == 4:
                    state = (9 if x['4']<12 else 10)
                    if state == 9:
                        return 0.953070343
                    if state == 10:
                        return 0.883813083
            if state == 2:
                state = (5 if x['7']<5 else 6)
                if state == 5:
                    state = (11 if x['8']<166 else 12)
                    if state == 11:
                        return -0.552006185
                    if state == 12:
                        return -4.93104315
                if state == 6:
                    state = (13 if x['10']<10 else 14)
                    if state == 13:
                        return -0.176490709
                    if state == 14:
                        return 0.00646844972
    elif num_booster == 36:
        state = 0
        if state == 0:
            state = (1 if x['3']<6 else 2)
            if state == 1:
                state = (3 if x['8']<330 else 4)
                if state == 3:
                    state = (7 if x['7']<320 else 8)
                    if state == 7:
                        return -0.00236066082
                    if state == 8:
                        return 4.3814435
                if state == 4:
                    state = (9 if x['1']<3 else 10)
                    if state == 9:
                        return 28.1849918
                    if state == 10:
                        return -11.1939497
            if state == 2:
                state = (5 if x['8']<330 else 6)
                if state == 5:
                    state = (11 if x['7']<320 else 12)
                    if state == 11:
                        return 0.00775662158
                    if state == 12:
                        return -5.23809242
                if state == 6:
                    state = (13 if x['6']<12 else 14)
                    if state == 13:
                        return -37.9217262
                    if state == 14:
                        return 32.9777222
    elif num_booster == 37:
        state = 0
        if state == 0:
            state = (1 if x['6']<21 else 2)
            if state == 1:
                state = (3 if x['8']<264 else 4)
                if state == 3:
                    state = (7 if x['8']<206 else 8)
                    if state == 7:
                        return 0.00317592616
                    if state == 8:
                        return 0.745056331
                if state == 4:
                    state = (9 if x['5']<45 else 10)
                    if state == 9:
                        return 1.31172764
                    if state == 10:
                        return 31.3792305
            if state == 2:
                state = (5 if x['8']<264 else 6)
                if state == 5:
                    state = (11 if x['7']<255 else 12)
                    if state == 11:
                        return -0.00390572287
                    if state == 12:
                        return -1.78270495
                if state == 6:
                    state = (13 if x['1']<12 else 14)
                    if state == 13:
                        return -6.15657854
                    if state == 14:
                        return 30.0901489
    elif num_booster == 38:
        state = 0
        if state == 0:
            state = (1 if x['7']<1542 else 2)
            if state == 1:
                state = (3 if x['7']<61 else 4)
                if state == 3:
                    state = (5 if x['7']<59 else 6)
                    if state == 5:
                        return -0.000411105342
                    if state == 6:
                        return 1.73940468
                if state == 4:
                    state = (7 if x['10']<10 else 8)
                    if state == 7:
                        return -0.766347528
                    if state == 8:
                        return 0.0357862562
            if state == 2:
                return 11.2354498
    elif num_booster == 39:
        state = 0
        if state == 0:
            state = (1 if x['9']<13 else 2)
            if state == 1:
                state = (3 if x['7']<320 else 4)
                if state == 3:
                    state = (7 if x['8']<330 else 8)
                    if state == 7:
                        return 0.0337459818
                    if state == 8:
                        return 5.68309689
                if state == 4:
                    state = (9 if x['6']<44 else 10)
                    if state == 9:
                        return -3.98337293
                    if state == 10:
                        return 30.963377
            if state == 2:
                state = (5 if x['8']<330 else 6)
                if state == 5:
                    state = (11 if x['7']<128 else 12)
                    if state == 11:
                        return -0.0269541536
                    if state == 12:
                        return 0.261877507
                if state == 6:
                    state = (13 if x['4']<4 else 14)
                    if state == 13:
                        return -75.0183792
                    if state == 14:
                        return 0.638612509
    elif num_booster == 40:
        state = 0
        if state == 0:
            state = (1 if x['7']<53 else 2)
            if state == 1:
                state = (3 if x['7']<47 else 4)
                if state == 3:
                    state = (7 if x['7']<44 else 8)
                    if state == 7:
                        return 0.0123067535
                    if state == 8:
                        return -0.710810602
                if state == 4:
                    state = (9 if x['7']<52 else 10)
                    if state == 9:
                        return 0.502147019
                    if state == 10:
                        return 1.05126786
            if state == 2:
                state = (5 if x['5']<44 else 6)
                if state == 5:
                    state = (11 if x['8']<330 else 12)
                    if state == 11:
                        return -0.0889640972
                    if state == 12:
                        return 3.9950664
                if state == 6:
                    state = (13 if x['8']<330 else 14)
                    if state == 13:
                        return -0.0675440133
                    if state == 14:
                        return -19.6108341
    elif num_booster == 41:
        state = 0
        if state == 0:
            state = (1 if x['7']<15 else 2)
            if state == 1:
                state = (3 if x['7']<13 else 4)
                if state == 3:
                    state = (7 if x['7']<12 else 8)
                    if state == 7:
                        return 0.000611308205
                    if state == 8:
                        return 0.589818299
                if state == 4:
                    state = (9 if x['7']<14 else 10)
                    if state == 9:
                        return -1.22542298
                    if state == 10:
                        return -0.427894324
            if state == 2:
                state = (5 if x['7']<18 else 6)
                if state == 5:
                    state = (11 if x['7']<17 else 12)
                    if state == 11:
                        return 0.27335164
                    if state == 12:
                        return 1.02350008
                if state == 6:
                    state = (13 if x['7']<19 else 14)
                    if state == 13:
                        return -1.05933845
                    if state == 14:
                        return 0.0175808482
    elif num_booster == 42:
        state = 0
        if state == 0:
            state = (1 if x['4']<7 else 2)
            if state == 1:
                state = (3 if x['7']<320 else 4)
                if state == 3:
                    state = (7 if x['8']<330 else 8)
                    if state == 7:
                        return -0.00654801587
                    if state == 8:
                        return -29.9411469
                if state == 4:
                    state = (9 if x['2']<31 else 10)
                    if state == 9:
                        return 6.32344103
                    if state == 10:
                        return 180.351364
            if state == 2:
                state = (5 if x['7']<320 else 6)
                if state == 5:
                    state = (11 if x['8']<330 else 12)
                    if state == 11:
                        return 0.00304781552
                    if state == 12:
                        return 8.83423519
                if state == 6:
                    state = (13 if x['1']<11 else 14)
                    if state == 13:
                        return -3.06009841
                    if state == 14:
                        return -38.7005272
    elif num_booster == 43:
        state = 0
        if state == 0:
            state = (1 if x['9']<6 else 2)
            if state == 1:
                state = (3 if x['8']<330 else 4)
                if state == 3:
                    state = (7 if x['8']<190 else 8)
                    if state == 7:
                        return -0.089399986
                    if state == 8:
                        return 5.15800524
                if state == 4:
                    state = (9 if x['5']<12 else 10)
                    if state == 9:
                        return -51.5550957
                    if state == 10:
                        return 108.987183
            if state == 2:
                state = (5 if x['7']<320 else 6)
                if state == 5:
                    state = (11 if x['10']<6 else 12)
                    if state == 11:
                        return 0.426381826
                    if state == 12:
                        return -0.00315565779
                if state == 6:
                    state = (13 if x['10']<6 else 14)
                    if state == 13:
                        return -46.4255486
                    if state == 14:
                        return 2.72553802
    elif num_booster == 44:
        state = 0
        if state == 0:
            state = (1 if x['1']<5 else 2)
            if state == 1:
                state = (3 if x['8']<330 else 4)
                if state == 3:
                    state = (7 if x['7']<112 else 8)
                    if state == 7:
                        return 0.0039333757
                    if state == 8:
                        return -0.474602759
                if state == 4:
                    state = (9 if x['4']<6 else 10)
                    if state == 9:
                        return 48.3454933
                    if state == 10:
                        return -11.2470312
            if state == 2:
                state = (5 if x['8']<330 else 6)
                if state == 5:
                    state = (11 if x['7']<82 else 12)
                    if state == 11:
                        return -0.00719160074
                    if state == 12:
                        return 0.180759341
                if state == 6:
                    state = (13 if x['2']<3 else 14)
                    if state == 13:
                        return 52.9116707
                    if state == 14:
                        return -2.97962165
    elif num_booster == 45:
        state = 0
        if state == 0:
            state = (1 if x['2']<8 else 2)
            if state == 1:
                state = (3 if x['7']<320 else 4)
                if state == 3:
                    state = (7 if x['7']<255 else 8)
                    if state == 7:
                        return -0.0297558401
                    if state == 8:
                        return 2.3067863
                if state == 4:
                    state = (9 if x['2']<7 else 10)
                    if state == 9:
                        return -3.68998528
                    if state == 10:
                        return -57.4597359
            if state == 2:
                state = (5 if x['7']<320 else 6)
                if state == 5:
                    state = (11 if x['7']<198 else 12)
                    if state == 11:
                        return 0.00917231571
                    if state == 12:
                        return -0.409020483
                if state == 6:
                    state = (13 if x['2']<12 else 14)
                    if state == 13:
                        return 44.2273026
                    if state == 14:
                        return -1.89138973
    elif num_booster == 46:
        state = 0
        if state == 0:
            state = (1 if x['2']<21 else 2)
            if state == 1:
                state = (3 if x['7']<320 else 4)
                if state == 3:
                    state = (7 if x['8']<330 else 8)
                    if state == 7:
                        return -0.00208303309
                    if state == 8:
                        return -3.97211146
                if state == 4:
                    state = (9 if x['5']<9 else 10)
                    if state == 9:
                        return -34.5772705
                    if state == 10:
                        return 1.65858352
            if state == 2:
                state = (5 if x['7']<320 else 6)
                if state == 5:
                    state = (11 if x['8']<330 else 12)
                    if state == 11:
                        return 0.00310320896
                    if state == 12:
                        return 5.85278463
                if state == 6:
                    state = (13 if x['10']<16 else 14)
                    if state == 13:
                        return 19.3674889
                    if state == 14:
                        return -18.0944176
    elif num_booster == 47:
        state = 0
        if state == 0:
            state = (1 if x['9']<7 else 2)
            if state == 1:
                state = (3 if x['8']<330 else 4)
                if state == 3:
                    state = (7 if x['8']<129 else 8)
                    if state == 7:
                        return -0.0499393605
                    if state == 8:
                        return 0.990890324
                if state == 4:
                    state = (9 if x['12']<10 else 10)
                    if state == 9:
                        return 38.7530594
                    if state == 10:
                        return -69.774498
            if state == 2:
                state = (5 if x['7']<320 else 6)
                if state == 5:
                    state = (11 if x['12']<3 else 12)
                    if state == 11:
                        return 0.0521220751
                    if state == 12:
                        return -0.0206818264
                if state == 6:
                    state = (13 if x['9']<16 else 14)
                    if state == 13:
                        return -9.49403667
                    if state == 14:
                        return 7.64947987
    elif num_booster == 48:
        state = 0
        if state == 0:
            state = (1 if x['12']<9 else 2)
            if state == 1:
                state = (3 if x['7']<320 else 4)
                if state == 3:
                    state = (7 if x['8']<330 else 8)
                    if state == 7:
                        return -0.000918375736
                    if state == 8:
                        return -4.48536062
                if state == 4:
                    state = (9 if x['5']<37 else 10)
                    if state == 9:
                        return 0.967539907
                    if state == 10:
                        return -16.8336563
            if state == 2:
                state = (5 if x['8']<330 else 6)
                if state == 5:
                    state = (11 if x['8']<264 else 12)
                    if state == 11:
                        return -0.00151712098
                    if state == 12:
                        return 1.50369143
                if state == 6:
                    state = (13 if x['3']<2 else 14)
                    if state == 13:
                        return 94.2281036
                    if state == 14:
                        return 3.71665168
    elif num_booster == 49:
        state = 0
        if state == 0:
            state = (1 if x['5']<22 else 2)
            if state == 1:
                state = (3 if x['7']<320 else 4)
                if state == 3:
                    state = (7 if x['8']<330 else 8)
                    if state == 7:
                        return -0.00132491894
                    if state == 8:
                        return 5.73166609
                if state == 4:
                    state = (9 if x['5']<21 else 10)
                    if state == 9:
                        return -3.22967339
                    if state == 10:
                        return -76.1618805
            if state == 2:
                state = (5 if x['7']<320 else 6)
                if state == 5:
                    state = (11 if x['8']<330 else 12)
                    if state == 11:
                        return 0.00139033212
                    if state == 12:
                        return -5.13080263
                if state == 6:
                    state = (13 if x['5']<28 else 14)
                    if state == 13:
                        return 39.6409111
                    if state == 14:
                        return 0.842654169
    elif num_booster == 50:
        state = 0
        if state == 0:
            state = (1 if x['12']<11 else 2)
            if state == 1:
                state = (3 if x['8']<330 else 4)
                if state == 3:
                    state = (7 if x['7']<0 else 8)
                    if state == 7:
                        return -0.0372584723
                    if state == 8:
                        return 0.013997701
                if state == 4:
                    state = (9 if x['5']<47 else 10)
                    if state == 9:
                        return -1.01919448
                    if state == 10:
                        return 31.5417976
            if state == 2:
                state = (5 if x['8']<330 else 6)
                if state == 5:
                    state = (11 if x['7']<320 else 12)
                    if state == 11:
                        return -0.0116057768
                    if state == 12:
                        return 4.29853249
                if state == 6:
                    state = (13 if x['6']<7 else 14)
                    if state == 13:
                        return -111.248962
                    if state == 14:
                        return -1.43479562
    elif num_booster == 51:
        state = 0
        if state == 0:
            state = (1 if x['7']<49 else 2)
            if state == 1:
                state = (3 if x['7']<46 else 4)
                if state == 3:
                    state = (7 if x['7']<44 else 8)
                    if state == 7:
                        return 0.011861573
                    if state == 8:
                        return -0.510820568
                if state == 4:
                    state = (9 if x['7']<48 else 10)
                    if state == 9:
                        return 0.299334288
                    if state == 10:
                        return 0.743410945
            if state == 2:
                state = (5 if x['12']<1 else 6)
                if state == 5:
                    state = (11 if x['7']<320 else 12)
                    if state == 11:
                        return 0.197173312
                    if state == 12:
                        return 6.23186493
                if state == 6:
                    state = (13 if x['2']<14 else 14)
                    if state == 13:
                        return -0.323149085
                    if state == 14:
                        return 0.00337952818
    elif num_booster == 52:
        state = 0
        if state == 0:
            state = (1 if x['7']<320 else 2)
            if state == 1:
                state = (3 if x['7']<64 else 4)
                if state == 3:
                    state = (7 if x['7']<61 else 8)
                    if state == 7:
                        return -0.000871452328
                    if state == 8:
                        return -0.884356797
                if state == 4:
                    state = (9 if x['10']<9 else 10)
                    if state == 9:
                        return -0.902478039
                    if state == 10:
                        return 0.188693002
            if state == 2:
                state = (5 if x['4']<14 else 6)
                if state == 5:
                    state = (11 if x['12']<3 else 12)
                    if state == 11:
                        return -11.9287443
                    if state == 12:
                        return 11.3514557
                if state == 6:
                    state = (13 if x['9']<21 else 14)
                    if state == 13:
                        return -14.4951601
                    if state == 14:
                        return 38.960556
    elif num_booster == 53:
        state = 0
        if state == 0:
            state = (1 if x['10']<12 else 2)
            if state == 1:
                state = (3 if x['7']<320 else 4)
                if state == 3:
                    state = (7 if x['7']<149 else 8)
                    if state == 7:
                        return 0.0407605283
                    if state == 8:
                        return -0.410078853
                if state == 4:
                    state = (9 if x['9']<16 else 10)
                    if state == 9:
                        return 2.16888022
                    if state == 10:
                        return 54.5971146
            if state == 2:
                state = (5 if x['7']<320 else 6)
                if state == 5:
                    state = (11 if x['8']<330 else 12)
                    if state == 11:
                        return -0.0157506503
                    if state == 12:
                        return -4.29512596
                if state == 6:
                    state = (13 if x['4']<7 else 14)
                    if state == 13:
                        return 7.61831284
                    if state == 14:
                        return -13.3276081
    elif num_booster == 54:
        state = 0
        if state == 0:
            state = (1 if x['7']<1542 else 2)
            if state == 1:
                state = (3 if x['2']<11 else 4)
                if state == 3:
                    state = (5 if x['7']<320 else 6)
                    if state == 5:
                        return 0.0261134245
                    if state == 6:
                        return 3.89621425
                if state == 4:
                    state = (7 if x['8']<330 else 8)
                    if state == 7:
                        return -0.00849370193
                    if state == 8:
                        return -2.31660271
            if state == 2:
                return -30.4583015
    elif num_booster == 55:
        state = 0
        if state == 0:
            state = (1 if x['7']<1542 else 2)
            if state == 1:
                state = (3 if x['7']<181 else 4)
                if state == 3:
                    state = (5 if x['7']<169 else 6)
                    if state == 5:
                        return 0.000350301067
                    if state == 6:
                        return -1.01268983
                if state == 4:
                    state = (7 if x['4']<12 else 8)
                    if state == 7:
                        return -1.70217168
                    if state == 8:
                        return 1.29064441
            if state == 2:
                return -18.2749996
    elif num_booster == 56:
        state = 0
        if state == 0:
            state = (1 if x['7']<1542 else 2)
            if state == 1:
                state = (3 if x['2']<2 else 4)
                if state == 3:
                    state = (5 if x['8']<330 else 6)
                    if state == 5:
                        return -0.0582088307
                    if state == 6:
                        return -10.6844473
                if state == 4:
                    state = (7 if x['8']<330 else 8)
                    if state == 7:
                        return 0.00197165273
                    if state == 8:
                        return 0.915052116
            if state == 2:
                return -10.9649906
    elif num_booster == 57:
        state = 0
        if state == 0:
            state = (1 if x['7']<21 else 2)
            if state == 1:
                state = (3 if x['7']<18 else 4)
                if state == 3:
                    state = (7 if x['10']<6 else 8)
                    if state == 7:
                        return -0.30303821
                    if state == 8:
                        return -0.00158499391
                if state == 4:
                    state = (9 if x['8']<190 else 10)
                    if state == 9:
                        return -0.231539831
                    if state == 10:
                        return 1.63048136
            if state == 2:
                state = (5 if x['10']<9 else 6)
                if state == 5:
                    state = (11 if x['7']<320 else 12)
                    if state == 11:
                        return 0.0293878652
                    if state == 12:
                        return 11.6823797
                if state == 6:
                    state = (13 if x['8']<330 else 14)
                    if state == 13:
                        return 0.0307652559
                    if state == 14:
                        return -3.6991818
    elif num_booster == 58:
        state = 0
        if state == 0:
            state = (1 if x['7']<1542 else 2)
            if state == 1:
                state = (3 if x['7']<198 else 4)
                if state == 3:
                    state = (5 if x['8']<228 else 6)
                    if state == 5:
                        return 0.00497444812
                    if state == 6:
                        return -2.57865572
                if state == 4:
                    state = (7 if x['11']<1 else 8)
                    if state == 7:
                        return -4.77416182
                    if state == 8:
                        return 0.158753842
            if state == 2:
                return -11.2519531
    elif num_booster == 59:
        state = 0
        if state == 0:
            state = (1 if x['7']<128 else 2)
            if state == 1:
                state = (3 if x['7']<88 else 4)
                if state == 3:
                    state = (7 if x['7']<85 else 8)
                    if state == 7:
                        return -0.00690580672
                    if state == 8:
                        return 2.15281367
                if state == 4:
                    state = (9 if x['7']<90 else 10)
                    if state == 9:
                        return -2.1952455
                    if state == 10:
                        return -0.0842375383
            if state == 2:
                state = (5 if x['1']<2 else 6)
                if state == 5:
                    state = (11 if x['9']<6 else 12)
                    if state == 11:
                        return 35.5939445
                    if state == 12:
                        return -2.21691346
                if state == 6:
                    state = (13 if x['9']<8 else 14)
                    if state == 13:
                        return -2.11209083
                    if state == 14:
                        return 0.440709352
    elif num_booster == 60:
        state = 0
        if state == 0:
            state = (1 if x['7']<68 else 2)
            if state == 1:
                state = (3 if x['7']<63 else 4)
                if state == 3:
                    state = (7 if x['7']<61 else 8)
                    if state == 7:
                        return 0.00141630252
                    if state == 8:
                        return -0.583645463
                if state == 4:
                    state = (9 if x['10']<10 else 10)
                    if state == 9:
                        return 1.86696851
                    if state == 10:
                        return 0.627378464
            if state == 2:
                state = (5 if x['12']<3 else 6)
                if state == 5:
                    state = (11 if x['10']<9 else 12)
                    if state == 11:
                        return -1.28524244
                    if state == 12:
                        return 0.530245125
                if state == 6:
                    state = (13 if x['5']<31 else 14)
                    if state == 13:
                        return -0.447859734
                    if state == 14:
                        return 0.166271165
    elif num_booster == 61:
        state = 0
        if state == 0:
            state = (1 if x['8']<228 else 2)
            if state == 1:
                state = (3 if x['7']<221 else 4)
                if state == 3:
                    state = (7 if x['8']<206 else 8)
                    if state == 7:
                        return -0.00163989572
                    if state == 8:
                        return 1.18515098
                if state == 4:
                    state = (9 if x['9']<16 else 10)
                    if state == 9:
                        return 1.03994
                    if state == 10:
                        return -3.24884725
            if state == 2:
                state = (5 if x['1']<8 else 6)
                if state == 5:
                    state = (11 if x['5']<47 else 12)
                    if state == 11:
                        return 0.482814163
                    if state == 12:
                        return 12.5867043
                if state == 6:
                    state = (13 if x['2']<29 else 14)
                    if state == 13:
                        return -4.49307203
                    if state == 14:
                        return 20.6212006
    elif num_booster == 62:
        state = 0
        if state == 0:
            state = (1 if x['5']<49 else 2)
            if state == 1:
                state = (3 if x['8']<228 else 4)
                if state == 3:
                    state = (7 if x['7']<198 else 8)
                    if state == 7:
                        return 0.00368829514
                    if state == 8:
                        return -0.53985101
                if state == 4:
                    state = (9 if x['4']<4 else 10)
                    if state == 9:
                        return -7.44708872
                    if state == 10:
                        return 1.08644497
            if state == 2:
                state = (5 if x['8']<228 else 6)
                if state == 5:
                    state = (11 if x['7']<221 else 12)
                    if state == 11:
                        return -0.0282585304
                    if state == 12:
                        return 1.88943851
                if state == 6:
                    state = (13 if x['10']<7 else 14)
                    if state == 13:
                        return -50.614666
                    if state == 14:
                        return -5.89630461
    elif num_booster == 63:
        state = 0
        if state == 0:
            state = (1 if x['7']<1542 else 2)
            if state == 1:
                state = (3 if x['12']<9 else 4)
                if state == 3:
                    state = (5 if x['8']<264 else 6)
                    if state == 5:
                        return -0.00678317109
                    if state == 6:
                        return -0.889309525
                if state == 4:
                    state = (7 if x['8']<330 else 8)
                    if state == 7:
                        return 0.023705693
                    if state == 8:
                        return 4.72222567
            if state == 2:
                return -7.37597656
    elif num_booster == 64:
        state = 0
        if state == 0:
            state = (1 if x['8']<166 else 2)
            if state == 1:
                state = (3 if x['7']<169 else 4)
                if state == 3:
                    state = (7 if x['10']<10 else 8)
                    if state == 7:
                        return 0.0470073819
                    if state == 8:
                        return -0.0177891068
                if state == 4:
                    state = (9 if x['1']<9 else 10)
                    if state == 9:
                        return -0.269105047
                    if state == 10:
                        return -2.178689
            if state == 2:
                state = (5 if x['3']<3 else 6)
                if state == 5:
                    state = (11 if x['10']<23 else 12)
                    if state == 11:
                        return -0.801504254
                    if state == 12:
                        return -49.0912476
                if state == 6:
                    state = (13 if x['5']<3 else 14)
                    if state == 13:
                        return -18.4817944
                    if state == 14:
                        return 0.715466022
    elif num_booster == 65:
        state = 0
        if state == 0:
            state = (1 if x['2']<26 else 2)
            if state == 1:
                state = (3 if x['7']<221 else 4)
                if state == 3:
                    state = (7 if x['7']<198 else 8)
                    if state == 7:
                        return 0.00667154649
                    if state == 8:
                        return -0.705184698
                if state == 4:
                    state = (9 if x['3']<7 else 10)
                    if state == 9:
                        return 2.05649352
                    if state == 10:
                        return -4.70958757
            if state == 2:
                state = (5 if x['8']<330 else 6)
                if state == 5:
                    state = (11 if x['7']<320 else 12)
                    if state == 11:
                        return -0.0234033372
                    if state == 12:
                        return 6.5976572
                if state == 6:
                    state = (13 if x['0']<3 else 14)
                    if state == 13:
                        return -19.7923145
                    if state == 14:
                        return 11.8730812
    elif num_booster == 66:
        state = 0
        if state == 0:
            state = (1 if x['7']<198 else 2)
            if state == 1:
                state = (3 if x['7']<181 else 4)
                if state == 3:
                    state = (7 if x['8']<264 else 8)
                    if state == 7:
                        return 0.000795790751
                    if state == 8:
                        return 4.47612095
                if state == 4:
                    state = (9 if x['9']<8 else 10)
                    if state == 9:
                        return 4.34561777
                    if state == 10:
                        return 0.435088485
            if state == 2:
                state = (5 if x['6']<4 else 6)
                if state == 5:
                    state = (11 if x['5']<45 else 12)
                    if state == 11:
                        return -0.353841931
                    if state == 12:
                        return 106.28125
                if state == 6:
                    state = (13 if x['12']<11 else 14)
                    if state == 13:
                        return -0.108821824
                    if state == 14:
                        return -4.33902884
    elif num_booster == 67:
        state = 0
        if state == 0:
            state = (1 if x['8']<330 else 2)
            if state == 1:
                state = (3 if x['7']<320 else 4)
                if state == 3:
                    state = (7 if x['8']<264 else 8)
                    if state == 7:
                        return 0.00169874041
                    if state == 8:
                        return -0.55366677
                if state == 4:
                    state = (9 if x['9']<19 else 10)
                    if state == 9:
                        return 7.89968634
                    if state == 10:
                        return -17.7642288
            if state == 2:
                state = (5 if x['1']<3 else 6)
                if state == 5:
                    state = (11 if x['6']<44 else 12)
                    if state == 11:
                        return -3.55123401
                    if state == 12:
                        return 60.604023
                if state == 6:
                    state = (13 if x['3']<3 else 14)
                    if state == 13:
                        return -14.9272413
                    if state == 14:
                        return 2.64393735
    elif num_booster == 68:
        state = 0
        if state == 0:
            state = (1 if x['7']<1542 else 2)
            if state == 1:
                state = (3 if x['5']<7 else 4)
                if state == 3:
                    state = (5 if x['7']<320 else 6)
                    if state == 5:
                        return 0.0211214107
                    if state == 6:
                        return 5.37476921
                if state == 4:
                    state = (7 if x['7']<320 else 8)
                    if state == 7:
                        return -0.00565727055
                    if state == 8:
                        return -1.31334424
            if state == 2:
                return 9.82636738
    elif num_booster == 69:
        state = 0
        if state == 0:
            state = (1 if x['6']<46 else 2)
            if state == 1:
                state = (3 if x['8']<264 else 4)
                if state == 3:
                    state = (7 if x['7']<255 else 8)
                    if state == 7:
                        return 0.000840632361
                    if state == 8:
                        return -0.687437534
                if state == 4:
                    state = (9 if x['10']<13 else 10)
                    if state == 9:
                        return -2.58463335
                    if state == 10:
                        return 3.35767555
            if state == 2:
                state = (5 if x['8']<264 else 6)
                if state == 5:
                    state = (11 if x['8']<228 else 12)
                    if state == 11:
                        return -0.0115716895
                    if state == 12:
                        return 2.89518499
                if state == 6:
                    state = (13 if x['12']<11 else 14)
                    if state == 13:
                        return -14.4048567
                    if state == 14:
                        return 47.4587784
    elif num_booster == 70:
        state = 0
        if state == 0:
            state = (1 if x['7']<1542 else 2)
            if state == 1:
                state = (3 if x['7']<78 else 4)
                if state == 3:
                    state = (5 if x['7']<68 else 6)
                    if state == 5:
                        return 0.00182966318
                    if state == 6:
                        return -0.394674629
                if state == 4:
                    state = (7 if x['7']<80 else 8)
                    if state == 7:
                        return 1.96428335
                    if state == 8:
                        return -0.00209254748
            if state == 2:
                return 6.92963886
    elif num_booster == 71:
        state = 0
        if state == 0:
            state = (1 if x['7']<85 else 2)
            if state == 1:
                state = (3 if x['7']<80 else 4)
                if state == 3:
                    state = (7 if x['8']<228 else 8)
                    if state == 7:
                        return 0.000420027965
                    if state == 8:
                        return -5.60959053
                if state == 4:
                    state = (9 if x['7']<82 else 10)
                    if state == 9:
                        return -1.80181777
                    if state == 10:
                        return 0.0276725795
            if state == 2:
                state = (5 if x['10']<6 else 6)
                if state == 5:
                    state = (11 if x['7']<320 else 12)
                    if state == 11:
                        return 8.01461983
                    if state == 12:
                        return -15.9617405
                if state == 6:
                    state = (13 if x['1']<11 else 14)
                    if state == 13:
                        return 0.151444942
                    if state == 14:
                        return -0.557753503
    elif num_booster == 72:
        state = 0
        if state == 0:
            state = (1 if x['8']<330 else 2)
            if state == 1:
                state = (3 if x['7']<320 else 4)
                if state == 3:
                    state = (7 if x['5']<47 else 8)
                    if state == 7:
                        return 0.0031849721
                    if state == 8:
                        return -0.0745654851
                if state == 4:
                    state = (9 if x['5']<39 else 10)
                    if state == 9:
                        return -5.22851849
                    if state == 10:
                        return 11.7515745
            if state == 2:
                state = (5 if x['6']<50 else 6)
                if state == 5:
                    state = (11 if x['6']<49 else 12)
                    if state == 11:
                        return 0.660584629
                    if state == 12:
                        return -35.068615
                if state == 6:
                    state = (13 if x['2']<2 else 14)
                    if state == 13:
                        return -70.8052902
                    if state == 14:
                        return 55.1324577
    elif num_booster == 73:
        state = 0
        if state == 0:
            state = (1 if x['3']<4 else 2)
            if state == 1:
                state = (3 if x['8']<330 else 4)
                if state == 3:
                    state = (7 if x['8']<156 else 8)
                    if state == 7:
                        return 0.0044342773
                    if state == 8:
                        return 0.281604856
                if state == 4:
                    state = (9 if x['0']<4 else 10)
                    if state == 9:
                        return -7.49683332
                    if state == 10:
                        return 25.9938755
            if state == 2:
                state = (5 if x['7']<320 else 6)
                if state == 5:
                    state = (11 if x['8']<264 else 12)
                    if state == 11:
                        return -0.00721383095
                    if state == 12:
                        return -0.668192089
                if state == 6:
                    state = (13 if x['4']<17 else 14)
                    if state == 13:
                        return -8.46186829
                    if state == 14:
                        return 29.6468849
    elif num_booster == 74:
        state = 0
        if state == 0:
            state = (1 if x['4']<15 else 2)
            if state == 1:
                state = (3 if x['7']<221 else 4)
                if state == 3:
                    state = (7 if x['7']<181 else 8)
                    if state == 7:
                        return -0.00385363377
                    if state == 8:
                        return 0.659078002
                if state == 4:
                    state = (9 if x['10']<18 else 10)
                    if state == 9:
                        return 3.46161079
                    if state == 10:
                        return -3.31671453
            if state == 2:
                state = (5 if x['7']<320 else 6)
                if state == 5:
                    state = (11 if x['7']<181 else 12)
                    if state == 11:
                        return 0.00300302636
                    if state == 12:
                        return -1.02637756
                if state == 6:
                    state = (13 if x['0']<3 else 14)
                    if state == 13:
                        return 14.2990313
                    if state == 14:
                        return -28.552536
    elif num_booster == 75:
        state = 0
        if state == 0:
            state = (1 if x['8']<330 else 2)
            if state == 1:
                state = (3 if x['8']<228 else 4)
                if state == 3:
                    state = (7 if x['7']<221 else 8)
                    if state == 7:
                        return 0.0024213763
                    if state == 8:
                        return -0.823614299
                if state == 4:
                    state = (9 if x['2']<31 else 10)
                    if state == 9:
                        return 0.140891001
                    if state == 10:
                        return -11.6418953
            if state == 2:
                state = (5 if x['2']<21 else 6)
                if state == 5:
                    state = (11 if x['12']<1 else 12)
                    if state == 11:
                        return 26.4206085
                    if state == 12:
                        return -7.20153522
                if state == 6:
                    state = (13 if x['5']<6 else 14)
                    if state == 13:
                        return 37.1604958
                    if state == 14:
                        return -0.776381195
    elif num_booster == 76:
        state = 0
        if state == 0:
            state = (1 if x['7']<1542 else 2)
            if state == 1:
                state = (3 if x['12']<3 else 4)
                if state == 3:
                    state = (5 if x['7']<255 else 6)
                    if state == 5:
                        return -0.0219039898
                    if state == 6:
                        return -2.22941446
                if state == 4:
                    state = (7 if x['7']<255 else 8)
                    if state == 7:
                        return 0.00767691201
                    if state == 8:
                        return 1.09102368
            if state == 2:
                return -9.41723633
    elif num_booster == 77:
        state = 0
        if state == 0:
            state = (1 if x['7']<1542 else 2)
            if state == 1:
                state = (3 if x['5']<10 else 4)
                if state == 3:
                    state = (5 if x['8']<330 else 6)
                    if state == 5:
                        return -0.000906610861
                    if state == 6:
                        return -7.9708457
                if state == 4:
                    state = (7 if x['8']<330 else 8)
                    if state == 7:
                        return -3.88634419
                    if state == 8:
                        return 3.33677292
            if state == 2:
                return -5.65034199
    elif num_booster == 78:
        state = 0
        if state == 0:
            state = (1 if x['10']<22 else 2)
            if state == 1:
                state = (3 if x['7']<100 else 4)
                if state == 3:
                    state = (7 if x['7']<57 else 8)
                    if state == 7:
                        return -0.00450320402
                    if state == 8:
                        return 0.0925647393
                if state == 4:
                    state = (9 if x['4']<12 else 10)
                    if state == 9:
                        return -0.525858045
                    if state == 10:
                        return 0.142029747
            if state == 2:
                state = (5 if x['7']<221 else 6)
                if state == 5:
                    state = (11 if x['8']<135 else 12)
                    if state == 11:
                        return 0.0108928001
                    if state == 12:
                        return 1.21445918
                if state == 6:
                    state = (13 if x['12']<9 else 14)
                    if state == 13:
                        return -10.5307837
                    if state == 14:
                        return 79.7869415
    elif num_booster == 79:
        state = 0
        if state == 0:
            state = (1 if x['7']<33 else 2)
            if state == 1:
                state = (3 if x['7']<32 else 4)
                if state == 3:
                    state = (7 if x['8']<264 else 8)
                    if state == 7:
                        return 0.00774920825
                    if state == 8:
                        return -6.6006732
                if state == 4:
                    return 0.57653141
            if state == 2:
                state = (5 if x['7']<34 else 6)
                if state == 5:
                    return -0.822164237
                if state == 6:
                    state = (9 if x['9']<13 else 10)
                    if state == 9:
                        return 0.146185622
                    if state == 10:
                        return -0.0579150021
    elif num_booster == 80:
        state = 0
        if state == 0:
            state = (1 if x['10']<11 else 2)
            if state == 1:
                state = (3 if x['7']<320 else 4)
                if state == 3:
                    state = (7 if x['8']<330 else 8)
                    if state == 7:
                        return -0.028593963
                    if state == 8:
                        return 8.18218613
                if state == 4:
                    state = (9 if x['2']<29 else 10)
                    if state == 9:
                        return -6.54751587
                    if state == 10:
                        return 32.1290092
            if state == 2:
                state = (5 if x['7']<320 else 6)
                if state == 5:
                    state = (11 if x['8']<190 else 12)
                    if state == 11:
                        return 0.00670336327
                    if state == 12:
                        return 0.330629706
                if state == 6:
                    state = (13 if x['6']<21 else 14)
                    if state == 13:
                        return 8.65575695
                    if state == 14:
                        return -5.78039503
    elif num_booster == 81:
        state = 0
        if state == 0:
            state = (1 if x['4']<14 else 2)
            if state == 1:
                state = (3 if x['8']<330 else 4)
                if state == 3:
                    state = (7 if x['7']<181 else 8)
                    if state == 7:
                        return 0.0144821582
                    if state == 8:
                        return 0.318070769
                if state == 4:
                    state = (9 if x['5']<47 else 10)
                    if state == 9:
                        return -1.3340838
                    if state == 10:
                        return 31.0906219
            if state == 2:
                state = (5 if x['8']<330 else 6)
                if state == 5:
                    state = (11 if x['8']<228 else 12)
                    if state == 11:
                        return -0.00914780982
                    if state == 12:
                        return -0.666131854
                if state == 6:
                    state = (13 if x['2']<22 else 14)
                    if state == 13:
                        return 8.69455528
                    if state == 14:
                        return -14.5445194
    elif num_booster == 82:
        state = 0
        if state == 0:
            state = (1 if x['7']<53 else 2)
            if state == 1:
                state = (3 if x['7']<42 else 4)
                if state == 3:
                    state = (7 if x['7']<40 else 8)
                    if state == 7:
                        return 0.00769241899
                    if state == 8:
                        return -0.409986347
                if state == 4:
                    state = (9 if x['7']<44 else 10)
                    if state == 9:
                        return 0.607143641
                    if state == 10:
                        return 0.0160887968
            if state == 2:
                state = (5 if x['5']<44 else 6)
                if state == 5:
                    state = (11 if x['8']<330 else 12)
                    if state == 11:
                        return -0.0119991107
                    if state == 12:
                        return 2.22134686
                if state == 6:
                    state = (13 if x['8']<330 else 14)
                    if state == 13:
                        return -0.192028865
                    if state == 14:
                        return -10.1020241
    elif num_booster == 83:
        state = 0
        if state == 0:
            state = (1 if x['7']<2 else 2)
            if state == 1:
                state = (3 if x['7']<1 else 4)
                if state == 3:
                    state = (7 if x['8']<264 else 8)
                    if state == 7:
                        return 0.000785708136
                    if state == 8:
                        return -8.72477245
                if state == 4:
                    state = (9 if x['10']<10 else 10)
                    if state == 9:
                        return -0.505587757
                    if state == 10:
                        return -0.431038111
            if state == 2:
                state = (5 if x['7']<4 else 6)
                if state == 5:
                    state = (11 if x['7']<3 else 12)
                    if state == 11:
                        return 0.346333116
                    if state == 12:
                        return 0.156255499
                if state == 6:
                    state = (13 if x['9']<6 else 14)
                    if state == 13:
                        return -0.19806546
                    if state == 14:
                        return 0.00646684086
    elif num_booster == 84:
        state = 0
        if state == 0:
            state = (1 if x['7']<0 else 2)
            if state == 1:
                state = (3 if x['7']<-1 else 4)
                if state == 3:
                    state = (7 if x['7']<-5 else 8)
                    if state == 7:
                        return -0.0504126251
                    if state == 8:
                        return 0.033806324
                if state == 4:
                    state = (9 if x['10']<10 else 10)
                    if state == 9:
                        return -0.305251688
                    if state == 10:
                        return -0.229871616
            if state == 2:
                state = (5 if x['7']<1 else 6)
                if state == 5:
                    state = (11 if x['9']<13 else 12)
                    if state == 11:
                        return 0.040620666
                    if state == 12:
                        return 0.112102173
                if state == 6:
                    state = (13 if x['6']<10 else 14)
                    if state == 13:
                        return -0.0395894833
                    if state == 14:
                        return 0.0156477001
    elif num_booster == 85:
        state = 0
        if state == 0:
            state = (1 if x['4']<4 else 2)
            if state == 1:
                state = (3 if x['8']<330 else 4)
                if state == 3:
                    state = (7 if x['8']<264 else 8)
                    if state == 7:
                        return 0.0829269066
                    if state == 8:
                        return 9.24084377
                if state == 4:
                    state = (9 if x['11']<2 else 10)
                    if state == 9:
                        return 83.3222275
                    if state == 10:
                        return -36.5401382
            if state == 2:
                state = (5 if x['8']<330 else 6)
                if state == 5:
                    state = (11 if x['4']<6 else 12)
                    if state == 11:
                        return -1.81467438
                    if state == 12:
                        return -7.97996472
                if state == 6:
                    state = (13 if x['4']<6 else 14)
                    if state == 13:
                        return 73.5770111
                    if state == 14:
                        return -0.581759393
    elif num_booster == 86:
        state = 0
        if state == 0:
            state = (1 if x['10']<6 else 2)
            if state == 1:
                state = (3 if x['7']<320 else 4)
                if state == 3:
                    state = (7 if x['7']<134 else 8)
                    if state == 7:
                        return -0.170734942
                    if state == 8:
                        return 5.69253063
                if state == 4:
                    state = (9 if x['2']<16 else 10)
                    if state == 9:
                        return 11.7115641
                    if state == 10:
                        return -51.0505524
            if state == 2:
                state = (5 if x['7']<320 else 6)
                if state == 5:
                    state = (11 if x['8']<264 else 12)
                    if state == 11:
                        return 0.00165083108
                    if state == 12:
                        return -0.435442924
                if state == 6:
                    state = (13 if x['6']<13 else 14)
                    if state == 13:
                        return -5.99444342
                    if state == 14:
                        return 3.70019984
    elif num_booster == 87:
        state = 0
        if state == 0:
            state = (1 if x['6']<2 else 2)
            if state == 1:
                state = (3 if x['7']<320 else 4)
                if state == 3:
                    state = (7 if x['8']<228 else 8)
                    if state == 7:
                        return 0.0184310004
                    if state == 8:
                        return -1.08106172
                if state == 4:
                    state = (9 if x['1']<5 else 10)
                    if state == 9:
                        return -16.5927982
                    if state == 10:
                        return 79.5265579
            if state == 2:
                state = (5 if x['8']<330 else 6)
                if state == 5:
                    state = (11 if x['6']<39 else 12)
                    if state == 11:
                        return 0.00977982488
                    if state == 12:
                        return -0.0300595965
                if state == 6:
                    state = (13 if x['5']<23 else 14)
                    if state == 13:
                        return -4.14232206
                    if state == 14:
                        return 3.94045258
    elif num_booster == 88:
        state = 0
        if state == 0:
            state = (1 if x['0']<2 else 2)
            if state == 1:
                state = (3 if x['7']<128 else 4)
                if state == 3:
                    state = (7 if x['8']<190 else 8)
                    if state == 7:
                        return 0.00881002937
                    if state == 8:
                        return -3.5057323
                if state == 4:
                    state = (9 if x['12']<5 else 10)
                    if state == 9:
                        return 1.38793993
                    if state == 10:
                        return -2.64343548
            if state == 2:
                state = (5 if x['7']<117 else 6)
                if state == 5:
                    state = (11 if x['8']<264 else 12)
                    if state == 11:
                        return -0.00460855057
                    if state == 12:
                        return 8.4010725
                if state == 6:
                    state = (13 if x['11']<9 else 14)
                    if state == 13:
                        return -0.0690641776
                    if state == 14:
                        return 1.14223015
    elif num_booster == 89:
        state = 0
        if state == 0:
            state = (1 if x['11']<5 else 2)
            if state == 1:
                state = (3 if x['8']<330 else 4)
                if state == 3:
                    state = (7 if x['8']<264 else 8)
                    if state == 7:
                        return 0.00965684839
                    if state == 8:
                        return 0.92884779
                if state == 4:
                    state = (9 if x['11']<4 else 10)
                    if state == 9:
                        return -5.08901358
                    if state == 10:
                        return 36.7849655
            if state == 2:
                state = (5 if x['8']<264 else 6)
                if state == 5:
                    state = (11 if x['7']<112 else 12)
                    if state == 11:
                        return 5.90843847
                    if state == 12:
                        return -0.141893983
                if state == 6:
                    state = (13 if x['5']<2 else 14)
                    if state == 13:
                        return 80.983696
                    if state == 14:
                        return -1.52610862
    elif num_booster == 90:
        state = 0
        if state == 0:
            state = (1 if x['7']<104 else 2)
            if state == 1:
                state = (3 if x['7']<100 else 4)
                if state == 3:
                    state = (7 if x['7']<90 else 8)
                    if state == 7:
                        return -0.00419580936
                    if state == 8:
                        return 0.200635433
                if state == 4:
                    state = (9 if x['10']<6 else 10)
                    if state == 9:
                        return -4.35032225
                    if state == 10:
                        return -0.732406616
            if state == 2:
                state = (5 if x['2']<7 else 6)
                if state == 5:
                    state = (11 if x['6']<33 else 12)
                    if state == 11:
                        return 0.432263911
                    if state == 12:
                        return -2.43712568
                if state == 6:
                    state = (13 if x['1']<8 else 14)
                    if state == 13:
                        return 0.53555125
                    if state == 14:
                        return -0.3615008
    elif num_booster == 91:
        state = 0
        if state == 0:
            state = (1 if x['6']<30 else 2)
            if state == 1:
                state = (3 if x['7']<320 else 4)
                if state == 3:
                    state = (7 if x['8']<264 else 8)
                    if state == 7:
                        return -0.0112207485
                    if state == 8:
                        return 1.01969743
                if state == 4:
                    state = (9 if x['2']<9 else 10)
                    if state == 9:
                        return 10.4484625
                    if state == 10:
                        return -7.61515951
            if state == 2:
                state = (5 if x['7']<320 else 6)
                if state == 5:
                    state = (11 if x['8']<264 else 12)
                    if state == 11:
                        return 0.0163876172
                    if state == 12:
                        return -1.12932527
                if state == 6:
                    state = (13 if x['1']<12 else 14)
                    if state == 13:
                        return -2.24983096
                    if state == 14:
                        return 56.00243
    elif num_booster == 92:
        state = 0
        if state == 0:
            state = (1 if x['12']<4 else 2)
            if state == 1:
                state = (3 if x['8']<330 else 4)
                if state == 3:
                    state = (7 if x['7']<320 else 8)
                    if state == 7:
                        return -0.0197095033
                    if state == 8:
                        return 6.34507608
                if state == 4:
                    state = (9 if x['11']<8 else 10)
                    if state == 9:
                        return -13.7320776
                    if state == 10:
                        return 18.5568962
            if state == 2:
                state = (5 if x['7']<128 else 6)
                if state == 5:
                    state = (11 if x['7']<53 else 12)
                    if state == 11:
                        return 0.01199101
                    if state == 12:
                        return -0.0769839883
                if state == 6:
                    state = (13 if x['1']<12 else 14)
                    if state == 13:
                        return 0.530880451
                    if state == 14:
                        return -2.56230855
    elif num_booster == 93:
        state = 0
        if state == 0:
            state = (1 if x['7']<134 else 2)
            if state == 1:
                state = (3 if x['7']<128 else 4)
                if state == 3:
                    state = (7 if x['7']<57 else 8)
                    if state == 7:
                        return -0.00376387476
                    if state == 8:
                        return 0.0770084709
                if state == 4:
                    state = (9 if x['1']<11 else 10)
                    if state == 9:
                        return 0.234566569
                    if state == 10:
                        return 2.69987941
            if state == 2:
                state = (5 if x['11']<7 else 6)
                if state == 5:
                    state = (11 if x['3']<7 else 12)
                    if state == 11:
                        return 0.779805005
                    if state == 12:
                        return -2.37590742
                if state == 6:
                    state = (13 if x['3']<6 else 14)
                    if state == 13:
                        return -1.7929306
                    if state == 14:
                        return 2.36539936
    elif num_booster == 94:
        state = 0
        if state == 0:
            state = (1 if x['7']<255 else 2)
            if state == 1:
                state = (3 if x['8']<330 else 4)
                if state == 3:
                    state = (7 if x['8']<264 else 8)
                    if state == 7:
                        return 0.000152516746
                    if state == 8:
                        return -2.1643734
                if state == 4:
                    state = (9 if x['5']<27 else 10)
                    if state == 9:
                        return 1.74197507
                    if state == 10:
                        return -25.1545849
            if state == 2:
                state = (5 if x['1']<11 else 6)
                if state == 5:
                    state = (11 if x['2']<19 else 12)
                    if state == 11:
                        return -0.984444559
                    if state == 12:
                        return 4.20560217
                if state == 6:
                    state = (13 if x['9']<6 else 14)
                    if state == 13:
                        return -64.9954453
                    if state == 14:
                        return -4.24641085
    elif num_booster == 95:
        state = 0
        if state == 0:
            state = (1 if x['8']<330 else 2)
            if state == 1:
                state = (3 if x['7']<320 else 4)
                if state == 3:
                    state = (7 if x['1']<11 else 8)
                    if state == 7:
                        return -0.0118061276
                    if state == 8:
                        return 0.0494386554
                if state == 4:
                    state = (9 if x['2']<4 else 10)
                    if state == 9:
                        return -17.3514996
                    if state == 10:
                        return 1.35308397
            if state == 2:
                state = (5 if x['6']<4 else 6)
                if state == 5:
                    state = (11 if x['11']<3 else 12)
                    if state == 11:
                        return -33.7965698
                    if state == 12:
                        return 94.8325195
                if state == 6:
                    state = (13 if x['5']<44 else 14)
                    if state == 13:
                        return 1.76129758
                    if state == 14:
                        return -10.5863791
    elif num_booster == 96:
        state = 0
        if state == 0:
            state = (1 if x['9']<7 else 2)
            if state == 1:
                state = (3 if x['8']<330 else 4)
                if state == 3:
                    state = (7 if x['8']<264 else 8)
                    if state == 7:
                        return 0.0900140926
                    if state == 8:
                        return -7.699615
                if state == 4:
                    state = (9 if x['11']<8 else 10)
                    if state == 9:
                        return 29.0021057
                    if state == 10:
                        return -45.5706291
            if state == 2:
                state = (5 if x['7']<320 else 6)
                if state == 5:
                    state = (11 if x['8']<330 else 12)
                    if state == 11:
                        return -0.00277623138
                    if state == 12:
                        return 4.24296188
                if state == 6:
                    state = (13 if x['10']<9 else 14)
                    if state == 13:
                        return 8.28434658
                    if state == 14:
                        return -3.26672554
    elif num_booster == 97:
        state = 0
        if state == 0:
            state = (1 if x['2']<15 else 2)
            if state == 1:
                state = (3 if x['8']<264 else 4)
                if state == 3:
                    state = (7 if x['7']<57 else 8)
                    if state == 7:
                        return -8.17265172
                    if state == 8:
                        return 0.113081358
                if state == 4:
                    state = (9 if x['2']<14 else 10)
                    if state == 9:
                        return -1.10858905
                    if state == 10:
                        return 33.1791191
            if state == 2:
                state = (5 if x['7']<255 else 6)
                if state == 5:
                    state = (11 if x['8']<330 else 12)
                    if state == 11:
                        return -0.00924943667
                    if state == 12:
                        return -7.61602783
                if state == 6:
                    state = (13 if x['9']<21 else 14)
                    if state == 13:
                        return -2.60026526
                    if state == 14:
                        return 11.1381235
    elif num_booster == 98:
        state = 0
        if state == 0:
            state = (1 if x['7']<53 else 2)
            if state == 1:
                state = (3 if x['7']<36 else 4)
                if state == 3:
                    state = (7 if x['7']<35 else 8)
                    if state == 7:
                        return 0.0087656239
                    if state == 8:
                        return -0.438386828
                if state == 4:
                    state = (9 if x['7']<40 else 10)
                    if state == 9:
                        return 0.307203621
                    if state == 10:
                        return 0.014075256
            if state == 2:
                state = (5 if x['7']<57 else 6)
                if state == 5:
                    state = (11 if x['7']<56 else 12)
                    if state == 11:
                        return -0.369670123
                    if state == 12:
                        return -0.921177804
                if state == 6:
                    state = (13 if x['3']<2 else 14)
                    if state == 13:
                        return 0.239880234
                    if state == 14:
                        return -0.077889055
    elif num_booster == 99:
        state = 0
        if state == 0:
            state = (1 if x['5']<48 else 2)
            if state == 1:
                state = (3 if x['7']<320 else 4)
                if state == 3:
                    state = (7 if x['8']<330 else 8)
                    if state == 7:
                        return -0.00270701014
                    if state == 8:
                        return 7.17995071
                if state == 4:
                    state = (9 if x['9']<12 else 10)
                    if state == 9:
                        return -6.48740768
                    if state == 10:
                        return 1.67370021
            if state == 2:
                state = (5 if x['7']<320 else 6)
                if state == 5:
                    state = (11 if x['8']<330 else 12)
                    if state == 11:
                        return 0.0390140079
                    if state == 12:
                        return -25.6016273
                if state == 6:
                    state = (13 if x['12']<5 else 14)
                    if state == 13:
                        return 34.2693825
                    if state == 14:
                        return -15.5160818

def xgb_predict(x):
    predict = 26.49534799453689
    # initialize prediction with base score
    for i in range(100):
        predict = predict + xgb_tree(x, i)
    return predict