import math

def xgb_tree(x, num_booster):
    if num_booster == 0:
        state = 0
        if state == 0:
            state = (1 if x['f8']<91 else 2)
            if state == 1:
                state = (3 if x['f7']<9 else 4)
                if state == 3:
                    state = (7 if x['f8']<36 else 8)
                    if state == 7:
                        return 4.67513227
                    if state == 8:
                        return 27.0151424
                if state == 4:
                    state = (9 if x['f8']<49 else 10)
                    if state == 9:
                        return -7.51154852
                    if state == 10:
                        return -1.03278708
            if state == 2:
                state = (5 if x['f7']<72 else 6)
                if state == 5:
                    state = (11 if x['f8']<120 else 12)
                    if state == 11:
                        return 44.3739967
                    if state == 12:
                        return 86.3258667
                if state == 6:
                    state = (13 if x['f8']<174 else 14)
                    if state == 13:
                        return 3.4865551
                    if state == 14:
                        return 18.4207668
    elif num_booster == 1:
        state = 0
        if state == 0:
            state = (1 if x['f4']<13 else 2)
            if state == 1:
                state = (3 if x['f8']<67 else 4)
                if state == 3:
                    state = (7 if x['f7']<28 else 8)
                    if state == 7:
                        return 2.39138484
                    if state == 8:
                        return -2.03657889
                if state == 4:
                    state = (9 if x['f7']<53 else 10)
                    if state == 9:
                        return 28.9028015
                    if state == 10:
                        return 3.43174052
            if state == 2:
                state = (5 if x['f7']<20 or math.isnan(x['f7'])  else 6)
                if state == 5:
                    state = (11 if x['f8']<54 else 12)
                    if state == 11:
                        return 0.515396178
                    if state == 12:
                        return 21.4698029
                if state == 6:
                    state = (13 if x['f8']<231 else 14)
                    if state == 13:
                        return -2.66214323
                    if state == 14:
                        return 9.26484203
    elif num_booster == 2:
        state = 0
        if state == 0:
            state = (1 if x['f8']<23 else 2)
            if state == 1:
                state = (3 if x['f8']<19 else 4)
                if state == 3:
                    state = (7 if x['f4']<14 else 8)
                    if state == 7:
                        return -4.44316149
                    if state == 8:
                        return -3.17576575
                if state == 4:
                    state = (9 if x['f7']<16 else 10)
                    if state == 9:
                        return -0.577440381
                    if state == 10:
                        return -2.96040344
            if state == 2:
                state = (5 if x['f7']<30 else 6)
                if state == 5:
                    state = (11 if x['f8']<43 else 12)
                    if state == 11:
                        return 2.85804534
                    if state == 12:
                        return 10.6089563
                if state == 6:
                    state = (13 if x['f6']<33 else 14)
                    if state == 13:
                        return 1.04232013
                    if state == 14:
                        return -3.20485282
    elif num_booster == 3:
        state = 0
        if state == 0:
            state = (1 if x['f6']<13 else 2)
            if state == 1:
                state = (3 if x['f7']<49 else 4)
                if state == 3:
                    state = (7 if x['f8']<56 else 8)
                    if state == 7:
                        return -0.537961125
                    if state == 8:
                        return 5.10031605
                if state == 4:
                    state = (9 if x['f4']<6 else 10)
                    if state == 9:
                        return 7.42858124
                    if state == 10:
                        return -5.06435585
            if state == 2:
                state = (5 if x['f8']<111 else 6)
                if state == 5:
                    state = (11 if x['f7']<83 or math.isnan(x['f7'])  else 12)
                    if state == 11:
                        return 0.472963184
                    if state == 12:
                        return -2.99089527
                if state == 6:
                    state = (13 if x['f7']<115 else 14)
                    if state == 13:
                        return 20.9312801
                    if state == 14:
                        return 2.4379313
    elif num_booster == 4:
        state = 0
        if state == 0:
            state = (1 if x['f10']<20 else 2)
            if state == 1:
                state = (3 if x['f8']<155 else 4)
                if state == 3:
                    state = (7 if x['f8']<29 else 8)
                    if state == 7:
                        return -0.858180642
                    if state == 8:
                        return 0.682232261
                if state == 4:
                    state = (9 if x['f7']<155 else 10)
                    if state == 9:
                        return 32.0531998
                    if state == 10:
                        return 0.0767022297
            if state == 2:
                state = (5 if x['f7']<180 else 6)
                if state == 5:
                    state = (11 if x['f7']<61 else 12)
                    if state == 11:
                        return -0.876995027
                    if state == 12:
                        return -4.00513983
                if state == 6:
                    state = (13 if x['f8']<426 else 14)
                    if state == 13:
                        return -18.6171227
                    if state == 14:
                        return 7.84480762
    elif num_booster == 5:
        state = 0
        if state == 0:
            state = (1 if x['f6']<33 else 2)
            if state == 1:
                state = (3 if x['f6']<30 else 4)
                if state == 3:
                    state = (7 if x['f6']<14 else 8)
                    if state == 7:
                        return 0.44233951
                    if state == 8:
                        return -2.27445793
                if state == 4:
                    state = (9 if x['f8']<73 else 10)
                    if state == 9:
                        return 2.12129164
                    if state == 10:
                        return 15.8561325
            if state == 2:
                state = (5 if x['f7']<107 else 6)
                if state == 5:
                    state = (11 if x['f8']<209 else 12)
                    if state == 11:
                        return -0.339385241
                    if state == 12:
                        return 51.5161629
                if state == 6:
                    state = (13 if x['f4']<20 else 14)
                    if state == 13:
                        return -8.60858917
                    if state == 14:
                        return 0.456024617
    elif num_booster == 6:
        state = 0
        if state == 0:
            state = (1 if x['f7']<328 or math.isnan(x['f7'])  else 2)
            if state == 1:
                state = (3 if x['f8']<334 else 4)
                if state == 3:
                    state = (7 if x['f8']<268 else 8)
                    if state == 7:
                        return -0.0612673871
                    if state == 8:
                        return 10.5059395
                if state == 4:
                    state = (9 if x['f5']<29 else 10)
                    if state == 9:
                        return 138.338882
                    if state == 10:
                        return 46.8190994
            if state == 2:
                state = (5 if x['f2']<7 else 6)
                if state == 5:
                    state = (11 if x['f6']<6 else 12)
                    if state == 11:
                        return 98.8882904
                    if state == 12:
                        return 2.91632819
                if state == 6:
                    state = (13 if x['f5']<53 else 14)
                    if state == 13:
                        return -14.2570972
                    if state == 14:
                        return 87.0401306
    elif num_booster == 7:
        state = 0
        if state == 0:
            state = (1 if x['f7']<227 or math.isnan(x['f7'])  else 2)
            if state == 1:
                state = (3 if x['f8']<231 else 4)
                if state == 3:
                    state = (7 if x['f7']<57 or math.isnan(x['f7'])  else 8)
                    if state == 7:
                        return 0.399151295
                    if state == 8:
                        return -0.907664895
                if state == 4:
                    state = (9 if x['f7']<188 else 10)
                    if state == 9:
                        return 73.6941605
                    if state == 10:
                        return 31.7138653
            if state == 2:
                state = (5 if x['f6']<14 else 6)
                if state == 5:
                    state = (11 if x['f6']<13 else 12)
                    if state == 11:
                        return -8.05403042
                    if state == 12:
                        return 30.3518066
                if state == 6:
                    state = (13 if x['f4']<20 else 14)
                    if state == 13:
                        return -12.0989809
                    if state == 14:
                        return -0.0798139125
    elif num_booster == 8:
        state = 0
        if state == 0:
            state = (1 if x['f9']<8 else 2)
            if state == 1:
                state = (3 if x['f7']<95 else 4)
                if state == 3:
                    state = (7 if x['f7']<50 else 8)
                    if state == 7:
                        return -0.576036155
                    if state == 8:
                        return -2.67127848
                if state == 4:
                    state = (9 if x['f4']<4 else 10)
                    if state == 9:
                        return 10.9974213
                    if state == 10:
                        return -9.70237637
            if state == 2:
                state = (5 if x['f8']<334 else 6)
                if state == 5:
                    state = (11 if x['f7']<242 or math.isnan(x['f7'])  else 12)
                    if state == 11:
                        return 0.141543865
                    if state == 12:
                        return -4.10873175
                if state == 6:
                    state = (13 if x['f7']<288 else 14)
                    if state == 13:
                        return 60.4949112
                    if state == 14:
                        return 5.11875439
    elif num_booster == 9:
        state = 0
        if state == 0:
            state = (1 if x['f8']<193 else 2)
            if state == 1:
                state = (3 if x['f7']<188 or math.isnan(x['f7'])  else 4)
                if state == 3:
                    state = (7 if x['f8']<186 else 8)
                    if state == 7:
                        return -0.067186676
                    if state == 8:
                        return 10.3328123
                if state == 4:
                    state = (9 if x['f10']<20 else 10)
                    if state == 9:
                        return -11.2826948
                    if state == 10:
                        return 7.17514372
            if state == 2:
                state = (5 if x['f7']<175 else 6)
                if state == 5:
                    state = (11 if x['f2']<6 else 12)
                    if state == 11:
                        return 3.75757837
                    if state == 12:
                        return 35.0563431
                if state == 6:
                    state = (13 if x['f2']<29 else 14)
                    if state == 13:
                        return 1.97058642
                    if state == 14:
                        return -8.55729866
    elif num_booster == 10:
        state = 0
        if state == 0:
            state = (1 if x['f6']<5 else 2)
            if state == 1:
                state = (3 if x['f8']<174 else 4)
                if state == 3:
                    state = (7 if x['f7']<109 else 8)
                    if state == 7:
                        return -1.27453876
                    if state == 8:
                        return -5.8132329
                if state == 4:
                    state = (9 if x['f7']<130 else 10)
                    if state == 9:
                        return 26.1539745
                    if state == 10:
                        return -18.7766132
            if state == 2:
                state = (5 if x['f8']<59 else 6)
                if state == 5:
                    state = (11 if x['f7']<44 or math.isnan(x['f7'])  else 12)
                    if state == 11:
                        return 0.0975051299
                    if state == 12:
                        return -1.39761376
                if state == 6:
                    state = (13 if x['f7']<9 else 14)
                    if state == 13:
                        return -5.95448923
                    if state == 14:
                        return 0.926907718
    elif num_booster == 11:
        state = 0
        if state == 0:
            state = (1 if x['f7']<103 or math.isnan(x['f7'])  else 2)
            if state == 1:
                state = (3 if x['f8']<78 else 4)
                if state == 3:
                    state = (7 if x['f7']<68 or math.isnan(x['f7'])  else 8)
                    if state == 7:
                        return 0.18289417
                    if state == 8:
                        return -2.89542127
                if state == 4:
                    state = (9 if x['f6']<31 else 10)
                    if state == 9:
                        return 3.73350596
                    if state == 10:
                        return -0.0108143529
            if state == 2:
                state = (5 if x['f8']<126 else 6)
                if state == 5:
                    state = (11 if x['f4']<7 else 12)
                    if state == 11:
                        return -6.23888397
                    if state == 12:
                        return -2.5017364
                if state == 6:
                    state = (13 if x['f7']<127 else 14)
                    if state == 13:
                        return 10.6924124
                    if state == 14:
                        return -0.648572624
    elif num_booster == 12:
        state = 0
        if state == 0:
            state = (1 if x['f8']<31 else 2)
            if state == 1:
                state = (3 if x['f7']<4 or math.isnan(x['f7'])  else 4)
                if state == 3:
                    state = (7 if x['f8']<26 else 8)
                    if state == 7:
                        return 0.218316302
                    if state == 8:
                        return 3.08194399
                if state == 4:
                    state = (9 if x['f7']<9 else 10)
                    if state == 9:
                        return -3.33783913
                    if state == 10:
                        return -0.789870918
            if state == 2:
                state = (5 if x['f7']<9 else 6)
                if state == 5:
                    state = (11 if x['f8']<36 else 12)
                    if state == 11:
                        return 5.38702536
                    if state == 12:
                        return -6.3543396
                if state == 6:
                    state = (13 if x['f7']<34 else 14)
                    if state == 13:
                        return 4.01352835
                    if state == 14:
                        return 0.116517983
    elif num_booster == 13:
        state = 0
        if state == 0:
            state = (1 if x['f5']<14 else 2)
            if state == 1:
                state = (3 if x['f5']<11 else 4)
                if state == 3:
                    state = (7 if x['f8']<426 else 8)
                    if state == 7:
                        return -0.0287933797
                    if state == 8:
                        return 17.8756599
                if state == 4:
                    state = (9 if x['f7']<92 else 10)
                    if state == 9:
                        return -0.727768779
                    if state == 10:
                        return -6.14535379
            if state == 2:
                state = (5 if x['f5']<44 else 6)
                if state == 5:
                    state = (11 if x['f8']<186 else 12)
                    if state == 11:
                        return 0.589874804
                    if state == 12:
                        return 6.28710127
                if state == 6:
                    state = (13 if x['f7']<425 else 14)
                    if state == 13:
                        return -0.583096862
                    if state == 14:
                        return -20.4595509
    elif num_booster == 14:
        state = 0
        if state == 0:
            state = (1 if x['f7']<204 or math.isnan(x['f7'])  else 2)
            if state == 1:
                state = (3 if x['f8']<200 else 4)
                if state == 3:
                    state = (7 if x['f8']<40 else 8)
                    if state == 7:
                        return -0.331209004
                    if state == 8:
                        return 0.46990028
                if state == 4:
                    state = (9 if x['f4']<20 else 10)
                    if state == 9:
                        return 2.33995605
                    if state == 10:
                        return 26.8848057
            if state == 2:
                state = (5 if x['f5']<53 else 6)
                if state == 5:
                    state = (11 if x['f10']<10 else 12)
                    if state == 11:
                        return -9.55829239
                    if state == 12:
                        return -0.677828729
                if state == 6:
                    state = (13 if x['f11']<6 else 14)
                    if state == 13:
                        return 138.210922
                    if state == 14:
                        return -23.2522163
    elif num_booster == 15:
        state = 0
        if state == 0:
            state = (1 if x['f7']<17 or math.isnan(x['f7'])  else 2)
            if state == 1:
                state = (3 if x['f8']<120 else 4)
                if state == 3:
                    state = (7 if x['f7']<9 else 8)
                    if state == 7:
                        return 0.0317553394
                    if state == 8:
                        return 2.22647452
                if state == 4:
                    state = (9 if x['f7']<4 else 10)
                    if state == 9:
                        return -10.5513487
                    if state == 10:
                        return -30.7228642
            if state == 2:
                state = (5 if x['f6']<30 else 6)
                if state == 5:
                    state = (11 if x['f6']<22 else 12)
                    if state == 11:
                        return -0.338064373
                    if state == 12:
                        return -2.04732728
                if state == 6:
                    state = (13 if x['f6']<31 else 14)
                    if state == 13:
                        return 13.0621786
                    if state == 14:
                        return -0.306503534
    elif num_booster == 16:
        state = 0
        if state == 0:
            state = (1 if x['f5']<50 else 2)
            if state == 1:
                state = (3 if x['f7']<139 or math.isnan(x['f7'])  else 4)
                if state == 3:
                    state = (7 if x['f8']<140 else 8)
                    if state == 7:
                        return -0.0341116041
                    if state == 8:
                        return 7.47326136
                if state == 4:
                    state = (9 if x['f4']<33 else 10)
                    if state == 9:
                        return 0.180179164
                    if state == 10:
                        return -5.79877377
            if state == 2:
                state = (5 if x['f8']<219 else 6)
                if state == 5:
                    state = (11 if x['f8']<61 else 12)
                    if state == 11:
                        return 0.727938294
                    if state == 12:
                        return 4.77421522
                if state == 6:
                    state = (13 if x['f6']<14 else 14)
                    if state == 13:
                        return 59.8852844
                    if state == 14:
                        return -9.79739666
    elif num_booster == 17:
        state = 0
        if state == 0:
            state = (1 if x['f4']<17 else 2)
            if state == 1:
                state = (3 if x['f8']<247 else 4)
                if state == 3:
                    state = (7 if x['f7']<215 else 8)
                    if state == 7:
                        return -0.345339775
                    if state == 8:
                        return -5.10855055
                if state == 4:
                    state = (9 if x['f1']<2 else 10)
                    if state == 9:
                        return -19.0740471
                    if state == 10:
                        return 4.2492156
            if state == 2:
                state = (5 if x['f4']<33 else 6)
                if state == 5:
                    state = (11 if x['f8']<64 else 12)
                    if state == 11:
                        return 0.49518466
                    if state == 12:
                        return 4.19180536
                if state == 6:
                    state = (13 if x['f7']<39 or math.isnan(x['f7'])  else 14)
                    if state == 13:
                        return 0.844326913
                    if state == 14:
                        return -0.797902465
    elif num_booster == 18:
        state = 0
        if state == 0:
            state = (1 if x['f4']<34 else 2)
            if state == 1:
                state = (3 if x['f7']<288 else 4)
                if state == 3:
                    state = (7 if x['f8']<293 else 8)
                    if state == 7:
                        return -0.0768892318
                    if state == 8:
                        return 20.77314
                if state == 4:
                    state = (9 if x['f3']<4 else 10)
                    if state == 9:
                        return -12.6689653
                    if state == 10:
                        return 2.22686124
            if state == 2:
                state = (5 if x['f8']<293 else 6)
                if state == 5:
                    state = (11 if x['f7']<111 or math.isnan(x['f7'])  else 12)
                    if state == 11:
                        return 1.11321771
                    if state == 12:
                        return -1.96633303
                if state == 6:
                    state = (13 if x['f6']<34 else 14)
                    if state == 13:
                        return 8.80029297
                    if state == 14:
                        return 86.1493301
    elif num_booster == 19:
        state = 0
        if state == 0:
            state = (1 if x['f8']<38 else 2)
            if state == 1:
                state = (3 if x['f7']<13 or math.isnan(x['f7'])  else 4)
                if state == 3:
                    state = (7 if x['f8']<36 else 8)
                    if state == 7:
                        return 0.395374656
                    if state == 8:
                        return -3.65533495
                if state == 4:
                    state = (9 if x['f8']<23 else 10)
                    if state == 9:
                        return 0.23472409
                    if state == 10:
                        return -1.16555655
            if state == 2:
                state = (5 if x['f7']<45 else 6)
                if state == 5:
                    state = (11 if x['f8']<91 else 12)
                    if state == 11:
                        return 2.13642478
                    if state == 12:
                        return -11.0400972
                if state == 6:
                    state = (13 if x['f11']<2 else 14)
                    if state == 13:
                        return 1.33477378
                    if state == 14:
                        return -0.339200854
    elif num_booster == 20:
        state = 0
        if state == 0:
            state = (1 if x['f7']<30 else 2)
            if state == 1:
                state = (3 if x['f6']<30 else 4)
                if state == 3:
                    state = (7 if x['f6']<16 else 8)
                    if state == 7:
                        return -0.229248166
                    if state == 8:
                        return 1.34461176
                if state == 4:
                    state = (9 if x['f8']<147 else 10)
                    if state == 9:
                        return -1.00198388
                    if state == 10:
                        return -17.0932789
            if state == 2:
                state = (5 if x['f6']<13 else 6)
                if state == 5:
                    state = (11 if x['f6']<11 else 12)
                    if state == 11:
                        return -0.0226799622
                    if state == 12:
                        return -5.08733511
                if state == 6:
                    state = (13 if x['f6']<14 else 14)
                    if state == 13:
                        return 2.71415973
                    if state == 14:
                        return 0.209858581
    elif num_booster == 21:
        state = 0
        if state == 0:
            state = (1 if x['f2']<31 else 2)
            if state == 1:
                state = (3 if x['f7']<90 or math.isnan(x['f7'])  else 4)
                if state == 3:
                    state = (7 if x['f8']<75 else 8)
                    if state == 7:
                        return -0.0530125275
                    if state == 8:
                        return 1.61533856
                if state == 4:
                    state = (9 if x['f8']<101 else 10)
                    if state == 9:
                        return -2.80596805
                    if state == 10:
                        return -0.0994376019
            if state == 2:
                state = (5 if x['f4']<13 else 6)
                if state == 5:
                    state = (11 if x['f8']<97 else 12)
                    if state == 11:
                        return 1.15084684
                    if state == 12:
                        return 16.1049309
                if state == 6:
                    state = (13 if x['f7']<242 else 14)
                    if state == 13:
                        return 0.438522786
                    if state == 14:
                        return -11.1138754
    elif num_booster == 22:
        state = 0
        if state == 0:
            state = (1 if x['f6']<38 else 2)
            if state == 1:
                state = (3 if x['f8']<426 else 4)
                if state == 3:
                    state = (7 if x['f8']<334 else 8)
                    if state == 7:
                        return -0.153234318
                    if state == 8:
                        return -7.28790426
                if state == 4:
                    state = (9 if x['f2']<7 else 10)
                    if state == 9:
                        return 28.2752972
                    if state == 10:
                        return -4.5273242
            if state == 2:
                state = (5 if x['f6']<39 else 6)
                if state == 5:
                    state = (11 if x['f7']<105 else 12)
                    if state == 11:
                        return 2.49402475
                    if state == 12:
                        return 15.8561478
                if state == 6:
                    state = (13 if x['f8']<111 else 14)
                    if state == 13:
                        return 0.334464163
                    if state == 14:
                        return -1.67866266
    elif num_booster == 23:
        state = 0
        if state == 0:
            state = (1 if x['f1']<2 else 2)
            if state == 1:
                state = (3 if x['f7']<120 else 4)
                if state == 3:
                    state = (7 if x['f8']<151 else 8)
                    if state == 7:
                        return -0.531699657
                    if state == 8:
                        return -14.4639606
                if state == 4:
                    state = (9 if x['f10']<6 else 10)
                    if state == 9:
                        return 37.8983116
                    if state == 10:
                        return -4.36460638
            if state == 2:
                state = (5 if x['f2']<23 else 6)
                if state == 5:
                    state = (11 if x['f7']<425 else 12)
                    if state == 11:
                        return 0.329256088
                    if state == 12:
                        return -6.28010511
                if state == 6:
                    state = (13 if x['f7']<328 or math.isnan(x['f7'])  else 14)
                    if state == 13:
                        return -0.309183538
                    if state == 14:
                        return 6.39766788
    elif num_booster == 24:
        state = 0
        if state == 0:
            state = (1 if x['f6']<6 else 2)
            if state == 1:
                state = (3 if x['f8']<113 else 4)
                if state == 3:
                    state = (7 if x['f10']<12 else 8)
                    if state == 7:
                        return 1.12921071
                    if state == 8:
                        return -0.256665289
                if state == 4:
                    state = (9 if x['f4']<4 else 10)
                    if state == 9:
                        return 34.1262589
                    if state == 10:
                        return 1.83502984
            if state == 2:
                state = (5 if x['f6']<10 else 6)
                if state == 5:
                    state = (11 if x['f7']<195 else 12)
                    if state == 11:
                        return -0.80436945
                    if state == 12:
                        return -10.91786
                if state == 6:
                    state = (13 if x['f9']<13 else 14)
                    if state == 13:
                        return -0.702045679
                    if state == 14:
                        return 0.289225399
    elif num_booster == 25:
        state = 0
        if state == 0:
            state = (1 if x['f4']<4 else 2)
            if state == 1:
                state = (3 if x['f7']<49 else 4)
                if state == 3:
                    state = (7 if x['f8']<97 else 8)
                    if state == 7:
                        return 0.458796471
                    if state == 8:
                        return -13.9617634
                if state == 4:
                    state = (9 if x['f3']<5 else 10)
                    if state == 9:
                        return -1.50266039
                    if state == 10:
                        return -9.80739594
            if state == 2:
                state = (5 if x['f4']<6 else 6)
                if state == 5:
                    state = (11 if x['f11']<9 else 12)
                    if state == 11:
                        return 2.04077911
                    if state == 12:
                        return 30.7214966
                if state == 6:
                    state = (13 if x['f8']<219 else 14)
                    if state == 13:
                        return -0.0233198609
                    if state == 14:
                        return 1.6948117
    elif num_booster == 26:
        state = 0
        if state == 0:
            state = (1 if x['f10']<16 else 2)
            if state == 1:
                state = (3 if x['f9']<17 else 4)
                if state == 3:
                    state = (7 if x['f6']<14 else 8)
                    if state == 7:
                        return 0.406000465
                    if state == 8:
                        return -0.398254663
                if state == 4:
                    state = (9 if x['f8']<426 else 10)
                    if state == 9:
                        return 1.05943406
                    if state == 10:
                        return 34.6936455
            if state == 2:
                state = (5 if x['f8']<174 else 6)
                if state == 5:
                    state = (11 if x['f7']<195 else 12)
                    if state == 11:
                        return -0.0859846398
                    if state == 12:
                        return 16.7084637
                if state == 6:
                    state = (13 if x['f11']<1 else 14)
                    if state == 13:
                        return 5.13626099
                    if state == 14:
                        return -4.0106616
    elif num_booster == 27:
        state = 0
        if state == 0:
            state = (1 if x['f8']<17 else 2)
            if state == 1:
                state = (3 if x['f7']<20 else 4)
                if state == 3:
                    state = (7 if x['f7']<13 else 8)
                    if state == 7:
                        return -0.870004773
                    if state == 8:
                        return -2.05030656
                if state == 4:
                    state = (9 if x['f6']<30 else 10)
                    if state == 9:
                        return 1.17536807
                    if state == 10:
                        return 0.0594782718
            if state == 2:
                state = (5 if x['f10']<20 else 6)
                if state == 5:
                    state = (11 if x['f9']<22 else 12)
                    if state == 11:
                        return 0.0426806025
                    if state == 12:
                        return -1.2789017
                if state == 6:
                    state = (13 if x['f8']<147 else 14)
                    if state == 13:
                        return 0.342415571
                    if state == 14:
                        return 4.03295088
    elif num_booster == 28:
        state = 0
        if state == 0:
            state = (1 if x['f7']<262 or math.isnan(x['f7'])  else 2)
            if state == 1:
                state = (3 if x['f8']<268 else 4)
                if state == 3:
                    state = (7 if x['f2']<10 else 8)
                    if state == 7:
                        return -0.32933417
                    if state == 8:
                        return 0.110026099
                if state == 4:
                    state = (9 if x['f7']<204 else 10)
                    if state == 9:
                        return -10.6575699
                    if state == 10:
                        return 21.385376
            if state == 2:
                state = (5 if x['f2']<3 else 6)
                if state == 5:
                    state = (11 if x['f10']<20 else 12)
                    if state == 11:
                        return -24.7763271
                    if state == 12:
                        return 45.694725
                if state == 6:
                    state = (13 if x['f11']<4 else 14)
                    if state == 13:
                        return -7.10086012
                    if state == 14:
                        return 4.26340294
    elif num_booster == 29:
        state = 0
        if state == 0:
            state = (1 if x['f10']<13 else 2)
            if state == 1:
                state = (3 if x['f9']<21 else 4)
                if state == 3:
                    state = (7 if x['f4']<26 else 8)
                    if state == 7:
                        return -0.13341324
                    if state == 8:
                        return 0.906158268
                if state == 4:
                    state = (9 if x['f8']<94 else 10)
                    if state == 9:
                        return 1.91276133
                    if state == 10:
                        return 24.9376507
            if state == 2:
                state = (5 if x['f1']<12 else 6)
                if state == 5:
                    state = (11 if x['f4']<8 else 12)
                    if state == 11:
                        return -0.483052224
                    if state == 12:
                        return 0.166072994
                if state == 6:
                    state = (13 if x['f8']<193 else 14)
                    if state == 13:
                        return -0.716612279
                    if state == 14:
                        return -10.7152824
    elif num_booster == 30:
        state = 0
        if state == 0:
            state = (1 if x['f4']<15 else 2)
            if state == 1:
                state = (3 if x['f5']<38 else 4)
                if state == 3:
                    state = (7 if x['f8']<165 else 8)
                    if state == 7:
                        return 0.409044713
                    if state == 8:
                        return 2.47641945
                if state == 4:
                    state = (9 if x['f8']<67 else 10)
                    if state == 9:
                        return 0.372034699
                    if state == 10:
                        return -2.79969144
            if state == 2:
                state = (5 if x['f8']<68 else 6)
                if state == 5:
                    state = (11 if x['f8']<49 else 12)
                    if state == 11:
                        return -0.184547111
                    if state == 12:
                        return -1.51684201
                if state == 6:
                    state = (13 if x['f7']<54 else 14)
                    if state == 13:
                        return 5.97988033
                    if state == 14:
                        return -0.0706420168
    elif num_booster == 31:
        state = 0
        if state == 0:
            state = (1 if x['f5']<47 else 2)
            if state == 1:
                state = (3 if x['f7']<122 or math.isnan(x['f7'])  else 4)
                if state == 3:
                    state = (7 if x['f8']<231 else 8)
                    if state == 7:
                        return 0.0302265827
                    if state == 8:
                        return -32.2400322
                if state == 4:
                    state = (9 if x['f5']<15 else 10)
                    if state == 9:
                        return -2.25093913
                    if state == 10:
                        return 0.528494596
            if state == 2:
                state = (5 if x['f1']<6 else 6)
                if state == 5:
                    state = (11 if x['f8']<71 else 12)
                    if state == 11:
                        return 0.552883744
                    if state == 12:
                        return 4.84987116
                if state == 6:
                    state = (13 if x['f4']<17 else 14)
                    if state == 13:
                        return 1.07503211
                    if state == 14:
                        return -1.70226991
    elif num_booster == 32:
        state = 0
        if state == 0:
            state = (1 if x['f9']<5 else 2)
            if state == 1:
                state = (3 if x['f7']<328 else 4)
                if state == 3:
                    state = (7 if x['f8']<231 else 8)
                    if state == 7:
                        return 1.29284608
                    if state == 8:
                        return -21.4102097
                if state == 4:
                    state = (9 if x['f9']<1 else 10)
                    if state == 9:
                        return 8.31581783
                    if state == 10:
                        return 152.077972
            if state == 2:
                state = (5 if x['f10']<23 else 6)
                if state == 5:
                    state = (11 if x['f8']<165 else 12)
                    if state == 11:
                        return -0.0492184721
                    if state == 12:
                        return 0.681357861
                if state == 6:
                    state = (13 if x['f8']<113 else 14)
                    if state == 13:
                        return -1.29113388
                    if state == 14:
                        return -17.7097816
    elif num_booster == 33:
        state = 0
        if state == 0:
            state = (1 if x['f7']<425 or math.isnan(x['f7'])  else 2)
            if state == 1:
                state = (3 if x['f8']<426 else 4)
                if state == 3:
                    state = (7 if x['f6']<49 else 8)
                    if state == 7:
                        return -0.0360396095
                    if state == 8:
                        return 0.615283966
                if state == 4:
                    state = (9 if x['f3']<4 else 10)
                    if state == 9:
                        return 7.39868164
                    if state == 10:
                        return 134.199814
            if state == 2:
                state = (5 if x['f11']<8 else 6)
                if state == 5:
                    state = (11 if x['f8']<426 else 12)
                    if state == 11:
                        return 44.1963539
                    if state == 12:
                        return -10.8669081
                if state == 6:
                    state = (13 if x['f3']<7 else 14)
                    if state == 13:
                        return -6.30344391
                    if state == 14:
                        return 64.0615463
    elif num_booster == 34:
        state = 0
        if state == 0:
            state = (1 if x['f5']<22 else 2)
            if state == 1:
                state = (3 if x['f3']<6 else 4)
                if state == 3:
                    state = (7 if x['f8']<247 else 8)
                    if state == 7:
                        return 0.266240984
                    if state == 8:
                        return 6.22818708
                if state == 4:
                    state = (9 if x['f7']<215 else 10)
                    if state == 9:
                        return -0.197785839
                    if state == 10:
                        return -7.29629135
            if state == 2:
                state = (5 if x['f5']<23 else 6)
                if state == 5:
                    state = (11 if x['f8']<174 else 12)
                    if state == 11:
                        return -1.59994102
                    if state == 12:
                        return -15.2023592
                if state == 6:
                    state = (13 if x['f2']<19 else 14)
                    if state == 13:
                        return 0.249232158
                    if state == 14:
                        return -0.269600898
    elif num_booster == 35:
        state = 0
        if state == 0:
            state = (1 if x['f3']<4 else 2)
            if state == 1:
                state = (3 if x['f8']<111 else 4)
                if state == 3:
                    state = (7 if x['f8']<103 else 8)
                    if state == 7:
                        return -0.10887748
                    if state == 8:
                        return 3.17493558
                if state == 4:
                    state = (9 if x['f7']<72 else 10)
                    if state == 9:
                        return -16.1143227
                    if state == 10:
                        return -0.358332962
            if state == 2:
                state = (5 if x['f10']<8 else 6)
                if state == 5:
                    state = (11 if x['f7']<107 else 12)
                    if state == 11:
                        return -0.078472659
                    if state == 12:
                        return -6.77530813
                if state == 6:
                    state = (13 if x['f8']<186 else 14)
                    if state == 13:
                        return 0.129942402
                    if state == 14:
                        return 2.1869247
    elif num_booster == 36:
        state = 0
        if state == 0:
            state = (1 if x['f7']<242 or math.isnan(x['f7'])  else 2)
            if state == 1:
                state = (3 if x['f8']<293 else 4)
                if state == 3:
                    state = (7 if x['f8']<247 else 8)
                    if state == 7:
                        return 0.0144118844
                    if state == 8:
                        return 5.81600285
                if state == 4:
                    state = (9 if x['f11']<3 else 10)
                    if state == 9:
                        return 76.9486389
                    if state == 10:
                        return 5.51093674
            if state == 2:
                state = (5 if x['f1']<8 else 6)
                if state == 5:
                    state = (11 if x['f3']<2 else 12)
                    if state == 11:
                        return 15.6390629
                    if state == 12:
                        return -0.862749219
                if state == 6:
                    state = (13 if x['f2']<26 else 14)
                    if state == 13:
                        return -11.2894716
                    if state == 14:
                        return 10.1339121
    elif num_booster == 37:
        state = 0
        if state == 0:
            state = (1 if x['f8']<56 else 2)
            if state == 1:
                state = (3 if x['f7']<69 else 4)
                if state == 3:
                    state = (7 if x['f8']<31 else 8)
                    if state == 7:
                        return 0.0343703739
                    if state == 8:
                        return -0.359659731
                if state == 4:
                    state = (9 if x['f8']<49 else 10)
                    if state == 9:
                        return 3.74627566
                    if state == 10:
                        return 0.434935868
            if state == 2:
                state = (5 if x['f7']<61 else 6)
                if state == 5:
                    state = (11 if x['f8']<91 else 12)
                    if state == 11:
                        return 2.93625641
                    if state == 12:
                        return -6.36344481
                if state == 6:
                    state = (13 if x['f8']<95 else 14)
                    if state == 13:
                        return -0.983372509
                    if state == 14:
                        return 0.530609071
    elif num_booster == 38:
        state = 0
        if state == 0:
            state = (1 if x['f7']<109 or math.isnan(x['f7'])  else 2)
            if state == 1:
                state = (3 if x['f8']<101 else 4)
                if state == 3:
                    state = (7 if x['f7']<78 or math.isnan(x['f7'])  else 8)
                    if state == 7:
                        return 0.060119886
                    if state == 8:
                        return -1.02098787
                if state == 4:
                    state = (9 if x['f7']<72 else 10)
                    if state == 9:
                        return -3.45597959
                    if state == 10:
                        return 6.14951086
            if state == 2:
                state = (5 if x['f2']<27 else 6)
                if state == 5:
                    state = (11 if x['f1']<11 else 12)
                    if state == 11:
                        return -0.712743104
                    if state == 12:
                        return 4.48894548
                if state == 6:
                    state = (13 if x['f7']<262 else 14)
                    if state == 13:
                        return -1.40398276
                    if state == 14:
                        return -9.41012955
    elif num_booster == 39:
        state = 0
        if state == 0:
            state = (1 if x['f7']<328 or math.isnan(x['f7'])  else 2)
            if state == 1:
                state = (3 if x['f8']<334 else 4)
                if state == 3:
                    state = (7 if x['f7']<94 or math.isnan(x['f7'])  else 8)
                    if state == 7:
                        return 0.0537756197
                    if state == 8:
                        return -0.379957736
                if state == 4:
                    state = (9 if x['f5']<11 else 10)
                    if state == 9:
                        return -68.2968597
                    if state == 10:
                        return 3.88825393
            if state == 2:
                state = (5 if x['f12']<5 else 6)
                if state == 5:
                    state = (11 if x['f3']<7 else 12)
                    if state == 11:
                        return -1.00645971
                    if state == 12:
                        return -19.4469948
                if state == 6:
                    state = (13 if x['f12']<6 else 14)
                    if state == 13:
                        return 33.1247215
                    if state == 14:
                        return 4.40393877
    elif num_booster == 40:
        state = 0
        if state == 0:
            state = (1 if x['f7']<30 or math.isnan(x['f7'])  else 2)
            if state == 1:
                state = (3 if x['f8']<97 else 4)
                if state == 3:
                    state = (7 if x['f8']<43 else 8)
                    if state == 7:
                        return -0.136586249
                    if state == 8:
                        return -2.04355574
                if state == 4:
                    state = (9 if x['f7']<15 else 10)
                    if state == 9:
                        return 17.2610569
                    if state == 10:
                        return -4.95400238
            if state == 2:
                state = (5 if x['f4']<8 else 6)
                if state == 5:
                    state = (11 if x['f7']<159 else 12)
                    if state == 11:
                        return 0.0925977528
                    if state == 12:
                        return -2.68511915
                if state == 6:
                    state = (13 if x['f4']<15 else 14)
                    if state == 13:
                        return 1.37043703
                    if state == 14:
                        return 0.097050719
    elif num_booster == 41:
        state = 0
        if state == 0:
            state = (1 if x['f8']<144 else 2)
            if state == 1:
                state = (3 if x['f7']<159 or math.isnan(x['f7'])  else 4)
                if state == 3:
                    state = (7 if x['f8']<91 else 8)
                    if state == 7:
                        return 0.00436766585
                    if state == 8:
                        return -0.62517637
                if state == 4:
                    state = (9 if x['f4']<29 else 10)
                    if state == 9:
                        return -10.2932501
                    if state == 10:
                        return 12.0757771
            if state == 2:
                state = (5 if x['f11']<2 else 6)
                if state == 5:
                    state = (11 if x['f2']<19 else 12)
                    if state == 11:
                        return 9.58654785
                    if state == 12:
                        return -2.04988575
                if state == 6:
                    state = (13 if x['f2']<15 else 14)
                    if state == 13:
                        return -1.72857475
                    if state == 14:
                        return 1.54271448
    elif num_booster == 42:
        state = 0
        if state == 0:
            state = (1 if x['f11']<8 else 2)
            if state == 1:
                state = (3 if x['f5']<11 else 4)
                if state == 3:
                    state = (7 if x['f7']<288 else 8)
                    if state == 7:
                        return -0.330992132
                    if state == 8:
                        return -7.57068491
                if state == 4:
                    state = (9 if x['f8']<293 else 10)
                    if state == 9:
                        return -0.00402486278
                    if state == 10:
                        return 4.58127642
            if state == 2:
                state = (5 if x['f8']<82 else 6)
                if state == 5:
                    state = (11 if x['f7']<90 or math.isnan(x['f7'])  else 12)
                    if state == 11:
                        return -0.0777229816
                    if state == 12:
                        return 2.43341708
                if state == 6:
                    state = (13 if x['f6']<14 else 14)
                    if state == 13:
                        return 2.75474286
                    if state == 14:
                        return 0.15256156
    elif num_booster == 43:
        state = 0
        if state == 0:
            state = (1 if x['f7']<94 or math.isnan(x['f7'])  else 2)
            if state == 1:
                state = (3 if x['f8']<97 else 4)
                if state == 3:
                    state = (7 if x['f7']<78 or math.isnan(x['f7'])  else 8)
                    if state == 7:
                        return 0.0889325663
                    if state == 8:
                        return -1.30472684
                if state == 4:
                    state = (9 if x['f7']<72 else 10)
                    if state == 9:
                        return -3.98984647
                    if state == 10:
                        return 9.24730968
            if state == 2:
                state = (5 if x['f6']<28 else 6)
                if state == 5:
                    state = (11 if x['f6']<14 else 12)
                    if state == 11:
                        return -0.252068222
                    if state == 12:
                        return -3.9311769
                if state == 6:
                    state = (13 if x['f6']<31 else 14)
                    if state == 13:
                        return 8.66808224
                    if state == 14:
                        return -0.452533811
    elif num_booster == 44:
        state = 0
        if state == 0:
            state = (1 if x['f4']<34 else 2)
            if state == 1:
                state = (3 if x['f8']<91 else 4)
                if state == 3:
                    state = (7 if x['f8']<85 else 8)
                    if state == 7:
                        return -0.0367815904
                    if state == 8:
                        return 2.31734896
                if state == 4:
                    state = (9 if x['f7']<7 else 10)
                    if state == 9:
                        return 9.17396832
                    if state == 10:
                        return -0.57097739
            if state == 2:
                state = (5 if x['f8']<65 else 6)
                if state == 5:
                    state = (11 if x['f6']<14 else 12)
                    if state == 11:
                        return -1.18379116
                    if state == 12:
                        return 0.0809597
                if state == 6:
                    state = (13 if x['f1']<11 else 14)
                    if state == 13:
                        return 3.62590647
                    if state == 14:
                        return -2.33201957
    elif num_booster == 45:
        state = 0
        if state == 0:
            state = (1 if x['f6']<3 else 2)
            if state == 1:
                state = (3 if x['f8']<48 else 4)
                if state == 3:
                    state = (7 if x['f7']<25 else 8)
                    if state == 7:
                        return 0.616263092
                    if state == 8:
                        return -1.5832175
                if state == 4:
                    state = (9 if x['f7']<31 else 10)
                    if state == 9:
                        return -11.3421888
                    if state == 10:
                        return -3.02339911
            if state == 2:
                state = (5 if x['f8']<129 else 6)
                if state == 5:
                    state = (11 if x['f8']<97 else 12)
                    if state == 11:
                        return 0.0212025847
                    if state == 12:
                        return -0.754789054
                if state == 6:
                    state = (13 if x['f5']<32 else 14)
                    if state == 13:
                        return 1.60298979
                    if state == 14:
                        return -1.23958588
    elif num_booster == 46:
        state = 0
        if state == 0:
            state = (1 if x['f5']<40 else 2)
            if state == 1:
                state = (3 if x['f5']<39 else 4)
                if state == 3:
                    state = (7 if x['f3']<7 else 8)
                    if state == 7:
                        return -0.114446558
                    if state == 8:
                        return 0.317643017
                if state == 4:
                    state = (9 if x['f8']<147 else 10)
                    if state == 9:
                        return -2.98039412
                    if state == 10:
                        return -14.014884
            if state == 2:
                state = (5 if x['f5']<43 else 6)
                if state == 5:
                    state = (11 if x['f8']<108 else 12)
                    if state == 11:
                        return 0.926153004
                    if state == 12:
                        return 9.4104681
                if state == 6:
                    state = (13 if x['f8']<334 else 14)
                    if state == 13:
                        return 0.0939891562
                    if state == 14:
                        return 6.91192627
    elif num_booster == 47:
        state = 0
        if state == 0:
            state = (1 if x['f3']<6 else 2)
            if state == 1:
                state = (3 if x['f8']<334 else 4)
                if state == 3:
                    state = (7 if x['f7']<328 or math.isnan(x['f7'])  else 8)
                    if state == 7:
                        return 0.107140698
                    if state == 8:
                        return -8.26011181
                if state == 4:
                    state = (9 if x['f10']<13 else 10)
                    if state == 9:
                        return 4.82534122
                    if state == 10:
                        return -13.2087173
            if state == 2:
                state = (5 if x['f8']<334 else 6)
                if state == 5:
                    state = (11 if x['f4']<13 else 12)
                    if state == 11:
                        return -0.727328479
                    if state == 12:
                        return 0.0273592658
                if state == 6:
                    state = (13 if x['f5']<6 else 14)
                    if state == 13:
                        return 47.1412086
                    if state == 14:
                        return 0.290614486
    elif num_booster == 48:
        state = 0
        if state == 0:
            state = (1 if x['f9']<18 else 2)
            if state == 1:
                state = (3 if x['f7']<425 or math.isnan(x['f7'])  else 4)
                if state == 3:
                    state = (7 if x['f6']<44 else 8)
                    if state == 7:
                        return -0.211072683
                    if state == 8:
                        return 0.384913683
                if state == 4:
                    state = (9 if x['f3']<7 else 10)
                    if state == 9:
                        return 0.971237957
                    if state == 10:
                        return -25.1123371
            if state == 2:
                state = (5 if x['f8']<426 else 6)
                if state == 5:
                    state = (11 if x['f7']<164 else 12)
                    if state == 11:
                        return 0.19131057
                    if state == 12:
                        return -1.14294159
                if state == 6:
                    state = (13 if x['f6']<6 else 14)
                    if state == 13:
                        return 82.3865204
                    if state == 14:
                        return -0.264720351
    elif num_booster == 49:
        state = 0
        if state == 0:
            state = (1 if x['f6']<22 else 2)
            if state == 1:
                state = (3 if x['f6']<21 else 4)
                if state == 3:
                    state = (7 if x['f6']<14 else 8)
                    if state == 7:
                        return 0.123945631
                    if state == 8:
                        return -1.2063539
                if state == 4:
                    state = (9 if x['f8']<83 else 10)
                    if state == 9:
                        return 1.92433345
                    if state == 10:
                        return 14.2494984
            if state == 2:
                state = (5 if x['f2']<31 else 6)
                if state == 5:
                    state = (11 if x['f7']<54 or math.isnan(x['f7'])  else 12)
                    if state == 11:
                        return 0.0168667659
                    if state == 12:
                        return -0.568812132
                if state == 6:
                    state = (13 if x['f8']<186 else 14)
                    if state == 13:
                        return 0.65703851
                    if state == 14:
                        return 12.5558672
    elif num_booster == 50:
        state = 0
        if state == 0:
            state = (1 if x['f8']<44 else 2)
            if state == 1:
                state = (3 if x['f7']<46 or math.isnan(x['f7'])  else 4)
                if state == 3:
                    state = (7 if x['f7']<24 or math.isnan(x['f7'])  else 8)
                    if state == 7:
                        return 0.155748665
                    if state == 8:
                        return -0.783026457
                if state == 4:
                    state = (9 if x['f8']<29 else 10)
                    if state == 9:
                        return 2.08406186
                    if state == 10:
                        return 0.808914185
            if state == 2:
                state = (5 if x['f7']<43 else 6)
                if state == 5:
                    state = (11 if x['f7']<30 else 12)
                    if state == 11:
                        return -0.325672477
                    if state == 12:
                        return 3.76095891
                if state == 6:
                    state = (13 if x['f2']<29 else 14)
                    if state == 13:
                        return 0.116300635
                    if state == 14:
                        return -0.876812637
    elif num_booster == 51:
        state = 0
        if state == 0:
            state = (1 if x['f7']<9 else 2)
            if state == 1:
                state = (3 if x['f7']<6 else 4)
                if state == 3:
                    state = (7 if x['f8']<38 else 8)
                    if state == 7:
                        return 0.275323838
                    if state == 8:
                        return -1.34625566
                if state == 4:
                    state = (9 if x['f8']<36 else 10)
                    if state == 9:
                        return -2.24306273
                    if state == 10:
                        return -8.34594631
            if state == 2:
                state = (5 if x['f7']<12 else 6)
                if state == 5:
                    state = (11 if x['f8']<26 else 12)
                    if state == 11:
                        return 0.992257714
                    if state == 12:
                        return 3.42198539
                if state == 6:
                    state = (13 if x['f6']<38 else 14)
                    if state == 13:
                        return -0.0616133623
                    if state == 14:
                        return 0.242475078
    elif num_booster == 52:
        state = 0
        if state == 0:
            state = (1 if x['f8']<16 else 2)
            if state == 1:
                state = (3 if x['f7']<24 else 4)
                if state == 3:
                    state = (7 if x['f4']<24 else 8)
                    if state == 7:
                        return -0.583070219
                    if state == 8:
                        return -1.06632364
                if state == 4:
                    state = (9 if x['f6']<29 else 10)
                    if state == 9:
                        return 0.776341856
                    if state == 10:
                        return -0.262867421
            if state == 2:
                state = (5 if x['f10']<6 else 6)
                if state == 5:
                    state = (11 if x['f11']<11 else 12)
                    if state == 11:
                        return 0.264654189
                    if state == 12:
                        return 14.1064596
                if state == 6:
                    state = (13 if x['f4']<26 else 14)
                    if state == 13:
                        return -0.0675592273
                    if state == 14:
                        return 0.182173297
    elif num_booster == 53:
        state = 0
        if state == 0:
            state = (1 if x['f2']<25 else 2)
            if state == 1:
                state = (3 if x['f2']<24 else 4)
                if state == 3:
                    state = (7 if x['f8']<113 else 8)
                    if state == 7:
                        return -0.110722713
                    if state == 8:
                        return 0.624757111
                if state == 4:
                    state = (9 if x['f7']<115 else 10)
                    if state == 9:
                        return -0.525611043
                    if state == 10:
                        return -5.14242268
            if state == 2:
                state = (5 if x['f8']<334 else 6)
                if state == 5:
                    state = (11 if x['f8']<174 else 12)
                    if state == 11:
                        return 0.202337846
                    if state == 12:
                        return -1.92410696
                if state == 6:
                    state = (13 if x['f9']<23 else 14)
                    if state == 13:
                        return 2.57954597
                    if state == 14:
                        return 71.429863
    elif num_booster == 54:
        state = 0
        if state == 0:
            state = (1 if x['f8']<334 else 2)
            if state == 1:
                state = (3 if x['f7']<159 or math.isnan(x['f7'])  else 4)
                if state == 3:
                    state = (7 if x['f8']<155 else 8)
                    if state == 7:
                        return 0.013637024
                    if state == 8:
                        return -3.29887438
                if state == 4:
                    state = (9 if x['f10']<6 else 10)
                    if state == 9:
                        return 22.4097538
                    if state == 10:
                        return 0.378498763
            if state == 2:
                state = (5 if x['f3']<3 else 6)
                if state == 5:
                    state = (11 if x['f7']<328 else 12)
                    if state == 11:
                        return -69.2510605
                    if state == 12:
                        return -8.1508131
                if state == 6:
                    state = (13 if x['f9']<16 else 14)
                    if state == 13:
                        return 6.33678389
                    if state == 14:
                        return -4.36069059
    elif num_booster == 55:
        state = 0
        if state == 0:
            state = (1 if x['f5']<4 else 2)
            if state == 1:
                state = (3 if x['f8']<151 else 4)
                if state == 3:
                    state = (7 if x['f8']<126 else 8)
                    if state == 7:
                        return -0.449291557
                    if state == 8:
                        return 6.64450932
                if state == 4:
                    state = (9 if x['f2']<27 else 10)
                    if state == 9:
                        return -8.64127254
                    if state == 10:
                        return 7.87290335
            if state == 2:
                state = (5 if x['f5']<5 else 6)
                if state == 5:
                    state = (11 if x['f7']<120 else 12)
                    if state == 11:
                        return 0.0892044827
                    if state == 12:
                        return 25.8913269
                if state == 6:
                    state = (13 if x['f1']<9 else 14)
                    if state == 13:
                        return 0.0869851038
                    if state == 14:
                        return -0.148063049
    elif num_booster == 56:
        state = 0
        if state == 0:
            state = (1 if x['f6']<50 else 2)
            if state == 1:
                state = (3 if x['f8']<165 else 4)
                if state == 3:
                    state = (7 if x['f7']<147 or math.isnan(x['f7'])  else 8)
                    if state == 7:
                        return 0.0174887981
                    if state == 8:
                        return -1.61401141
                if state == 4:
                    state = (9 if x['f5']<53 else 10)
                    if state == 9:
                        return 0.428294957
                    if state == 10:
                        return 30.5823822
            if state == 2:
                state = (5 if x['f7']<288 else 6)
                if state == 5:
                    state = (11 if x['f9']<13 else 12)
                    if state == 11:
                        return 2.14878583
                    if state == 12:
                        return -1.16626358
                if state == 6:
                    state = (13 if x['f5']<12 else 14)
                    if state == 13:
                        return -33.9324303
                    if state == 14:
                        return -8.51617336
    elif num_booster == 57:
        state = 0
        if state == 0:
            state = (1 if x['f7']<180 or math.isnan(x['f7'])  else 2)
            if state == 1:
                state = (3 if x['f8']<231 else 4)
                if state == 3:
                    state = (7 if x['f8']<165 else 8)
                    if state == 7:
                        return -0.00530970143
                    if state == 8:
                        return 2.4846437
                if state == 4:
                    state = (9 if x['f8']<247 else 10)
                    if state == 9:
                        return -24.154398
                    if state == 10:
                        return 3.92852139
            if state == 2:
                state = (5 if x['f12']<8 else 6)
                if state == 5:
                    state = (11 if x['f12']<6 else 12)
                    if state == 11:
                        return -0.895240724
                    if state == 12:
                        return 4.66909838
                if state == 6:
                    state = (13 if x['f1']<5 else 14)
                    if state == 13:
                        return 2.12170172
                    if state == 14:
                        return -4.99565315
    elif num_booster == 58:
        state = 0
        if state == 0:
            state = (1 if x['f5']<51 else 2)
            if state == 1:
                state = (3 if x['f1']<5 else 4)
                if state == 3:
                    state = (7 if x['f4']<26 else 8)
                    if state == 7:
                        return -0.330528766
                    if state == 8:
                        return 0.277384371
                if state == 4:
                    state = (9 if x['f9']<15 else 10)
                    if state == 9:
                        return -0.179297984
                    if state == 10:
                        return 0.203820229
            if state == 2:
                state = (5 if x['f7']<262 else 6)
                if state == 5:
                    state = (11 if x['f7']<242 else 12)
                    if state == 11:
                        return -0.901946723
                    if state == 12:
                        return -76.0180283
                if state == 6:
                    state = (13 if x['f12']<6 else 14)
                    if state == 13:
                        return 32.319397
                    if state == 14:
                        return -28.7998657
    elif num_booster == 59:
        state = 0
        if state == 0:
            state = (1 if x['f4']<33 else 2)
            if state == 1:
                state = (3 if x['f4']<26 else 4)
                if state == 3:
                    state = (7 if x['f6']<10 else 8)
                    if state == 7:
                        return -0.316778779
                    if state == 8:
                        return 0.160657272
                if state == 4:
                    state = (9 if x['f7']<45 else 10)
                    if state == 9:
                        return -0.631703258
                    if state == 10:
                        return 6.25284481
            if state == 2:
                state = (5 if x['f8']<219 else 6)
                if state == 5:
                    state = (11 if x['f8']<209 else 12)
                    if state == 11:
                        return -0.170009241
                    if state == 12:
                        return 6.89644194
                if state == 6:
                    state = (13 if x['f9']<10 else 14)
                    if state == 13:
                        return 15.2780142
                    if state == 14:
                        return -6.06140137
    elif num_booster == 60:
        state = 0
        if state == 0:
            state = (1 if x['f8']<219 else 2)
            if state == 1:
                state = (3 if x['f7']<195 or math.isnan(x['f7'])  else 4)
                if state == 3:
                    state = (7 if x['f8']<200 else 8)
                    if state == 7:
                        return -0.00905842707
                    if state == 8:
                        return 4.66112757
                if state == 4:
                    state = (9 if x['f4']<13 else 10)
                    if state == 9:
                        return -5.37034369
                    if state == 10:
                        return -0.205841333
            if state == 2:
                state = (5 if x['f5']<48 else 6)
                if state == 5:
                    state = (11 if x['f5']<47 else 12)
                    if state == 11:
                        return 1.19575417
                    if state == 12:
                        return 24.664669
                if state == 6:
                    state = (13 if x['f4']<4 else 14)
                    if state == 13:
                        return 28.2801247
                    if state == 14:
                        return -8.0972929
    elif num_booster == 61:
        state = 0
        if state == 0:
            state = (1 if x['f7']<40 or math.isnan(x['f7'])  else 2)
            if state == 1:
                state = (3 if x['f8']<47 else 4)
                if state == 3:
                    state = (7 if x['f6']<30 else 8)
                    if state == 7:
                        return 0.311543345
                    if state == 8:
                        return -0.367250025
                if state == 4:
                    state = (9 if x['f4']<7 else 10)
                    if state == 9:
                        return 2.72753716
                    if state == 10:
                        return 0.0359718762
            if state == 2:
                state = (5 if x['f9']<11 else 6)
                if state == 5:
                    state = (11 if x['f6']<33 else 12)
                    if state == 11:
                        return -1.84577906
                    if state == 12:
                        return 1.32339597
                if state == 6:
                    state = (13 if x['f7']<188 else 14)
                    if state == 13:
                        return -0.143738791
                    if state == 14:
                        return 0.988839924
    elif num_booster == 62:
        state = 0
        if state == 0:
            state = (1 if x['f8']<268 else 2)
            if state == 1:
                state = (3 if x['f7']<288 or math.isnan(x['f7'])  else 4)
                if state == 3:
                    state = (7 if x['f10']<12 else 8)
                    if state == 7:
                        return 0.300733387
                    if state == 8:
                        return -0.095390521
                if state == 4:
                    state = (9 if x['f10']<19 else 10)
                    if state == 9:
                        return -12.4991884
                    if state == 10:
                        return 92.2074432
            if state == 2:
                state = (5 if x['f10']<14 else 6)
                if state == 5:
                    state = (11 if x['f11']<5 else 12)
                    if state == 11:
                        return 3.00709629
                    if state == 12:
                        return -9.00557041
                if state == 6:
                    state = (13 if x['f5']<10 else 14)
                    if state == 13:
                        return 11.8855848
                    if state == 14:
                        return -1.48154593
    elif num_booster == 63:
        state = 0
        if state == 0:
            state = (1 if x['f10']<20 else 2)
            if state == 1:
                state = (3 if x['f10']<19 else 4)
                if state == 3:
                    state = (7 if x['f8']<219 else 8)
                    if state == 7:
                        return -0.0211428367
                    if state == 8:
                        return 0.790855825
                if state == 4:
                    state = (9 if x['f8']<165 else 10)
                    if state == 9:
                        return -0.139921755
                    if state == 10:
                        return -5.6485734
            if state == 2:
                state = (5 if x['f4']<4 else 6)
                if state == 5:
                    state = (11 if x['f8']<91 else 12)
                    if state == 11:
                        return -1.00977385
                    if state == 12:
                        return -11.4535751
                if state == 6:
                    state = (13 if x['f7']<62 else 14)
                    if state == 13:
                        return -0.0368582644
                    if state == 14:
                        return 1.23421609
    elif num_booster == 64:
        state = 0
        if state == 0:
            state = (1 if x['f7']<425 or math.isnan(x['f7'])  else 2)
            if state == 1:
                state = (3 if x['f8']<426 else 4)
                if state == 3:
                    state = (7 if x['f5']<47 else 8)
                    if state == 7:
                        return -0.0260501243
                    if state == 8:
                        return 0.430060953
                if state == 4:
                    state = (9 if x['f0']<4 else 10)
                    if state == 9:
                        return -11.3902006
                    if state == 10:
                        return 54.5618973
            if state == 2:
                state = (5 if x['f5']<33 else 6)
                if state == 5:
                    state = (11 if x['f5']<19 else 12)
                    if state == 11:
                        return -4.83275509
                    if state == 12:
                        return 16.7871647
                if state == 6:
                    state = (13 if x['f2']<13 else 14)
                    if state == 13:
                        return -23.5126019
                    if state == 14:
                        return -3.04099989
    elif num_booster == 65:
        state = 0
        if state == 0:
            state = (1 if x['f11']<4 else 2)
            if state == 1:
                state = (3 if x['f8']<231 else 4)
                if state == 3:
                    state = (7 if x['f8']<219 else 8)
                    if state == 7:
                        return -0.0821530744
                    if state == 8:
                        return 10.1360731
                if state == 4:
                    state = (9 if x['f6']<29 else 10)
                    if state == 9:
                        return -6.82315207
                    if state == 10:
                        return 1.1973114
            if state == 2:
                state = (5 if x['f8']<293 else 6)
                if state == 5:
                    state = (11 if x['f5']<53 else 12)
                    if state == 11:
                        return 0.0481105782
                    if state == 12:
                        return -5.4136939
                if state == 6:
                    state = (13 if x['f6']<14 else 14)
                    if state == 13:
                        return 9.29025936
                    if state == 14:
                        return -3.65619659
    elif num_booster == 66:
        state = 0
        if state == 0:
            state = (1 if x['f11']<10 else 2)
            if state == 1:
                state = (3 if x['f7']<328 or math.isnan(x['f7'])  else 4)
                if state == 3:
                    state = (7 if x['f8']<293 else 8)
                    if state == 7:
                        return 0.0427867472
                    if state == 8:
                        return 4.82630301
                if state == 4:
                    state = (9 if x['f11']<9 else 10)
                    if state == 9:
                        return -0.570449054
                    if state == 10:
                        return -17.7591877
            if state == 2:
                state = (5 if x['f7']<328 else 6)
                if state == 5:
                    state = (11 if x['f7']<227 else 12)
                    if state == 11:
                        return -0.148840979
                    if state == 12:
                        return -8.33125591
                if state == 6:
                    state = (13 if x['f4']<13 else 14)
                    if state == 13:
                        return 29.2630615
                    if state == 14:
                        return -11.1946344
    elif num_booster == 67:
        state = 0
        if state == 0:
            state = (1 if x['f9']<5 else 2)
            if state == 1:
                state = (3 if x['f7']<328 else 4)
                if state == 3:
                    state = (7 if x['f7']<262 else 8)
                    if state == 7:
                        return 0.575461328
                    if state == 8:
                        return -15.7787161
                if state == 4:
                    state = (9 if x['f5']<6 else 10)
                    if state == 9:
                        return 60.0160904
                    if state == 10:
                        return 1.04472935
            if state == 2:
                state = (5 if x['f10']<21 else 6)
                if state == 5:
                    state = (11 if x['f7']<188 or math.isnan(x['f7'])  else 12)
                    if state == 11:
                        return -0.00867099874
                    if state == 12:
                        return 0.622633696
                if state == 6:
                    state = (13 if x['f7']<262 else 14)
                    if state == 13:
                        return -0.284731627
                    if state == 14:
                        return -13.4014359
    elif num_booster == 68:
        state = 0
        if state == 0:
            state = (1 if x['f8']<174 else 2)
            if state == 1:
                state = (3 if x['f7']<175 or math.isnan(x['f7'])  else 4)
                if state == 3:
                    state = (7 if x['f6']<29 else 8)
                    if state == 7:
                        return 0.101365685
                    if state == 8:
                        return -0.118352808
                if state == 4:
                    state = (9 if x['f5']<7 else 10)
                    if state == 9:
                        return -8.2246809
                    if state == 10:
                        return 4.84231281
            if state == 2:
                state = (5 if x['f12']<3 else 6)
                if state == 5:
                    state = (11 if x['f4']<7 else 12)
                    if state == 11:
                        return -2.63777876
                    if state == 12:
                        return 4.30053091
                if state == 6:
                    state = (13 if x['f12']<4 else 14)
                    if state == 13:
                        return -8.53892803
                    if state == 14:
                        return -0.387582988
    elif num_booster == 69:
        state = 0
        if state == 0:
            state = (1 if x['f6']<33 else 2)
            if state == 1:
                state = (3 if x['f8']<79 else 4)
                if state == 3:
                    state = (7 if x['f7']<85 or math.isnan(x['f7'])  else 8)
                    if state == 7:
                        return -0.0345883779
                    if state == 8:
                        return 2.06369853
                if state == 4:
                    state = (9 if x['f9']<12 else 10)
                    if state == 9:
                        return -2.07090616
                    if state == 10:
                        return -0.109663621
            if state == 2:
                state = (5 if x['f7']<115 else 6)
                if state == 5:
                    state = (11 if x['f8']<111 else 12)
                    if state == 11:
                        return 0.171854302
                    if state == 12:
                        return -6.03614378
                if state == 6:
                    state = (13 if x['f4']<17 else 14)
                    if state == 13:
                        return 2.80064678
                    if state == 14:
                        return -1.76615083
    elif num_booster == 70:
        state = 0
        if state == 0:
            state = (1 if x['f9']<9 else 2)
            if state == 1:
                state = (3 if x['f8']<209 else 4)
                if state == 3:
                    state = (7 if x['f5']<28 else 8)
                    if state == 7:
                        return 0.730820835
                    if state == 8:
                        return -0.623430133
                if state == 4:
                    state = (9 if x['f3']<2 else 10)
                    if state == 9:
                        return 20.5436821
                    if state == 10:
                        return -0.234565213
            if state == 2:
                state = (5 if x['f7']<164 or math.isnan(x['f7'])  else 6)
                if state == 5:
                    state = (11 if x['f7']<159 or math.isnan(x['f7'])  else 12)
                    if state == 11:
                        return -0.0141933132
                    if state == 12:
                        return 3.73588252
                if state == 6:
                    state = (13 if x['f1']<11 else 14)
                    if state == 13:
                        return -1.02956271
                    if state == 14:
                        return 2.65943193
    elif num_booster == 71:
        state = 0
        if state == 0:
            state = (1 if x['f7']<204 else 2)
            if state == 1:
                state = (3 if x['f8']<231 else 4)
                if state == 3:
                    state = (7 if x['f8']<219 else 8)
                    if state == 7:
                        return -0.0159711689
                    if state == 8:
                        return 9.16760921
                if state == 4:
                    state = (9 if x['f5']<10 else 10)
                    if state == 9:
                        return 17.3892403
                    if state == 10:
                        return -10.8118267
            if state == 2:
                state = (5 if x['f4']<34 else 6)
                if state == 5:
                    state = (11 if x['f4']<14 else 12)
                    if state == 11:
                        return 1.56927454
                    if state == 12:
                        return -1.84136188
                if state == 6:
                    state = (13 if x['f3']<7 else 14)
                    if state == 13:
                        return 3.56691933
                    if state == 14:
                        return 31.508707
    elif num_booster == 72:
        state = 0
        if state == 0:
            state = (1 if x['f6']<7 else 2)
            if state == 1:
                state = (3 if x['f7']<49 else 4)
                if state == 3:
                    state = (7 if x['f8']<67 else 8)
                    if state == 7:
                        return -0.153215721
                    if state == 8:
                        return -3.63698196
                if state == 4:
                    state = (9 if x['f1']<9 else 10)
                    if state == 9:
                        return 1.77423942
                    if state == 10:
                        return -0.836035192
            if state == 2:
                state = (5 if x['f6']<10 else 6)
                if state == 5:
                    state = (11 if x['f8']<426 else 12)
                    if state == 11:
                        return -0.492877454
                    if state == 12:
                        return -16.6657925
                if state == 6:
                    state = (13 if x['f6']<11 else 14)
                    if state == 13:
                        return 0.852669835
                    if state == 14:
                        return -0.0280717108
    elif num_booster == 73:
        state = 0
        if state == 0:
            state = (1 if x['f8']<49 else 2)
            if state == 1:
                state = (3 if x['f7']<51 or math.isnan(x['f7'])  else 4)
                if state == 3:
                    state = (7 if x['f8']<47 else 8)
                    if state == 7:
                        return 0.00916509051
                    if state == 8:
                        return 1.04020703
                if state == 4:
                    state = (9 if x['f6']<13 else 10)
                    if state == 9:
                        return 1.81990802
                    if state == 10:
                        return 0.441072702
            if state == 2:
                state = (5 if x['f8']<52 else 6)
                if state == 5:
                    state = (11 if x['f7']<37 else 12)
                    if state == 11:
                        return 1.00737238
                    if state == 12:
                        return -2.21394086
                if state == 6:
                    state = (13 if x['f3']<5 else 14)
                    if state == 13:
                        return 0.176035836
                    if state == 14:
                        return -0.350682586
    elif num_booster == 74:
        state = 0
        if state == 0:
            state = (1 if x['f7']<122 else 2)
            if state == 1:
                state = (3 if x['f7']<101 else 4)
                if state == 3:
                    state = (7 if x['f8']<106 else 8)
                    if state == 7:
                        return -0.0398408547
                    if state == 8:
                        return 1.61919808
                if state == 4:
                    state = (9 if x['f8']<101 else 10)
                    if state == 9:
                        return 2.84530592
                    if state == 10:
                        return -0.136137053
            if state == 2:
                state = (5 if x['f9']<22 else 6)
                if state == 5:
                    state = (11 if x['f6']<29 else 12)
                    if state == 11:
                        return -0.715835989
                    if state == 12:
                        return 0.901976407
                if state == 6:
                    state = (13 if x['f6']<40 else 14)
                    if state == 13:
                        return -3.44453049
                    if state == 14:
                        return 2.93863916
    elif num_booster == 75:
        state = 0
        if state == 0:
            state = (1 if x['f8']<38 else 2)
            if state == 1:
                state = (3 if x['f8']<34 else 4)
                if state == 3:
                    state = (7 if x['f8']<31 else 8)
                    if state == 7:
                        return 0.0995951667
                    if state == 8:
                        return -0.592899382
                if state == 4:
                    state = (9 if x['f7']<25 else 10)
                    if state == 9:
                        return 1.99693799
                    if state == 10:
                        return 0.154740557
            if state == 2:
                state = (5 if x['f6']<14 else 6)
                if state == 5:
                    state = (11 if x['f8']<115 else 12)
                    if state == 11:
                        return -0.292096943
                    if state == 12:
                        return 1.58483052
                if state == 6:
                    state = (13 if x['f9']<15 else 14)
                    if state == 13:
                        return -1.12596595
                    if state == 14:
                        return 0.084983632
    elif num_booster == 76:
        state = 0
        if state == 0:
            state = (1 if x['f7']<83 else 2)
            if state == 1:
                state = (3 if x['f8']<120 else 4)
                if state == 3:
                    state = (7 if x['f8']<75 else 8)
                    if state == 7:
                        return 0.0259837061
                    if state == 8:
                        return 1.49209177
                if state == 4:
                    state = (9 if x['f7']<72 else 10)
                    if state == 9:
                        return -8.20393467
                    if state == 10:
                        return 12.308116
            if state == 2:
                state = (5 if x['f8']<69 else 6)
                if state == 5:
                    state = (11 if x['f2']<16 else 12)
                    if state == 11:
                        return 1.17077613
                    if state == 12:
                        return 3.85558581
                if state == 6:
                    state = (13 if x['f2']<8 else 14)
                    if state == 13:
                        return -1.29583347
                    if state == 14:
                        return -0.175783023
    elif num_booster == 77:
        state = 0
        if state == 0:
            state = (1 if x['f10']<15 else 2)
            if state == 1:
                state = (3 if x['f12']<9 else 4)
                if state == 3:
                    state = (7 if x['f7']<328 else 8)
                    if state == 7:
                        return 0.0409313403
                    if state == 8:
                        return -3.55925822
                if state == 4:
                    state = (9 if x['f8']<426 else 10)
                    if state == 9:
                        return 0.43893829
                    if state == 10:
                        return 18.7664204
            if state == 2:
                state = (5 if x['f7']<215 else 6)
                if state == 5:
                    state = (11 if x['f8']<193 else 12)
                    if state == 11:
                        return -0.125584111
                    if state == 12:
                        return -3.20491481
                if state == 6:
                    state = (13 if x['f6']<30 else 14)
                    if state == 13:
                        return -1.27583683
                    if state == 14:
                        return 4.58814764
    elif num_booster == 78:
        state = 0
        if state == 0:
            state = (1 if x['f12']<10 else 2)
            if state == 1:
                state = (3 if x['f8']<334 else 4)
                if state == 3:
                    state = (7 if x['f7']<328 else 8)
                    if state == 7:
                        return 0.0378287174
                    if state == 8:
                        return -6.60706472
                if state == 4:
                    state = (9 if x['f12']<9 else 10)
                    if state == 9:
                        return 0.649632156
                    if state == 10:
                        return 14.7624884
            if state == 2:
                state = (5 if x['f7']<425 else 6)
                if state == 5:
                    state = (11 if x['f8']<268 else 12)
                    if state == 11:
                        return -0.147738889
                    if state == 12:
                        return -4.55648661
                if state == 6:
                    state = (13 if x['f2']<29 else 14)
                    if state == 13:
                        return -24.4174252
                    if state == 14:
                        return 38.0680161
    elif num_booster == 79:
        state = 0
        if state == 0:
            state = (1 if x['f8']<120 else 2)
            if state == 1:
                state = (3 if x['f8']<111 else 4)
                if state == 3:
                    state = (7 if x['f4']<14 else 8)
                    if state == 7:
                        return -0.190441415
                    if state == 8:
                        return 0.106477015
                if state == 4:
                    state = (9 if x['f2']<6 else 10)
                    if state == 9:
                        return 4.11551428
                    if state == 10:
                        return -2.37087703
            if state == 2:
                state = (5 if x['f6']<31 else 6)
                if state == 5:
                    state = (11 if x['f6']<30 else 12)
                    if state == 11:
                        return 0.358985186
                    if state == 12:
                        return 14.7505713
                if state == 6:
                    state = (13 if x['f6']<32 else 14)
                    if state == 13:
                        return -18.9426327
                    if state == 14:
                        return -0.620149672
    elif num_booster == 80:
        state = 0
        if state == 0:
            state = (1 if x['f4']<9 else 2)
            if state == 1:
                state = (3 if x['f7']<139 else 4)
                if state == 3:
                    state = (7 if x['f8']<134 else 8)
                    if state == 7:
                        return 0.206154227
                    if state == 8:
                        return 5.45779228
                if state == 4:
                    state = (9 if x['f2']<6 else 10)
                    if state == 9:
                        return 6.18643332
                    if state == 10:
                        return -2.69623327
            if state == 2:
                state = (5 if x['f7']<17 else 6)
                if state == 5:
                    state = (11 if x['f8']<54 else 12)
                    if state == 11:
                        return -0.32203123
                    if state == 12:
                        return -5.01619911
                if state == 6:
                    state = (13 if x['f8']<23 else 14)
                    if state == 13:
                        return 0.588496149
                    if state == 14:
                        return -0.0350742117
    elif num_booster == 81:
        state = 0
        if state == 0:
            state = (1 if x['f8']<27 else 2)
            if state == 1:
                state = (3 if x['f7']<28 else 4)
                if state == 3:
                    state = (7 if x['f7']<14 else 8)
                    if state == 7:
                        return -0.0298677143
                    if state == 8:
                        return -0.695267856
                if state == 4:
                    state = (9 if x['f4']<18 else 10)
                    if state == 9:
                        return 0.7276752
                    if state == 10:
                        return -0.116117656
            if state == 2:
                state = (5 if x['f7']<24 else 6)
                if state == 5:
                    state = (11 if x['f8']<104 else 12)
                    if state == 11:
                        return 0.803061426
                    if state == 12:
                        return 5.05097389
                if state == 6:
                    state = (13 if x['f7']<30 else 14)
                    if state == 13:
                        return -0.816708684
                    if state == 14:
                        return -0.0225470848
    elif num_booster == 82:
        state = 0
        if state == 0:
            state = (1 if x['f7']<188 else 2)
            if state == 1:
                state = (3 if x['f7']<180 else 4)
                if state == 3:
                    state = (7 if x['f8']<293 else 8)
                    if state == 7:
                        return -0.00999506563
                    if state == 8:
                        return 26.7068157
                if state == 4:
                    state = (9 if x['f8']<219 else 10)
                    if state == 9:
                        return -1.77749002
                    if state == 10:
                        return -23.3895321
            if state == 2:
                state = (5 if x['f5']<12 else 6)
                if state == 5:
                    state = (11 if x['f10']<8 else 12)
                    if state == 11:
                        return 9.11983109
                    if state == 12:
                        return -2.67396235
                if state == 6:
                    state = (13 if x['f2']<4 else 14)
                    if state == 13:
                        return -5.78646517
                    if state == 14:
                        return 2.40082669
    elif num_booster == 83:
        state = 0
        if state == 0:
            state = (1 if x['f10']<20 else 2)
            if state == 1:
                state = (3 if x['f7']<74 else 4)
                if state == 3:
                    state = (7 if x['f8']<91 else 8)
                    if state == 7:
                        return 0.0662229732
                    if state == 8:
                        return -2.91423893
                if state == 4:
                    state = (9 if x['f8']<90 else 10)
                    if state == 9:
                        return -1.0343293
                    if state == 10:
                        return 0.0846996531
            if state == 2:
                state = (5 if x['f6']<12 else 6)
                if state == 5:
                    state = (11 if x['f7']<139 else 12)
                    if state == 11:
                        return 0.666698873
                    if state == 12:
                        return 5.27442312
                if state == 6:
                    state = (13 if x['f6']<33 else 14)
                    if state == 13:
                        return -0.674223602
                    if state == 14:
                        return 0.600493193
    elif num_booster == 84:
        state = 0
        if state == 0:
            state = (1 if x['f8']<46 else 2)
            if state == 1:
                state = (3 if x['f6']<14 else 4)
                if state == 3:
                    state = (7 if x['f6']<12 else 8)
                    if state == 7:
                        return -0.103788033
                    if state == 8:
                        return -1.14720941
                if state == 4:
                    state = (9 if x['f6']<30 else 10)
                    if state == 9:
                        return 0.554333866
                    if state == 10:
                        return -0.187434599
            if state == 2:
                state = (5 if x['f7']<46 else 6)
                if state == 5:
                    state = (11 if x['f4']<17 else 12)
                    if state == 11:
                        return 0.0586747825
                    if state == 12:
                        return 2.30572128
                if state == 6:
                    state = (13 if x['f11']<4 else 14)
                    if state == 13:
                        return -0.326437652
                    if state == 14:
                        return 0.190541908
    elif num_booster == 85:
        state = 0
        if state == 0:
            state = (1 if x['f7']<1542 or math.isnan(x['f7'])  else 2)
            if state == 1:
                state = (3 if x['f6']<33 else 4)
                if state == 3:
                    state = (5 if x['f6']<29 else 6)
                    if state == 5:
                        return 0.0239655729
                    if state == 6:
                        return -0.570419252
                if state == 4:
                    state = (7 if x['f8']<169 else 8)
                    if state == 7:
                        return 0.205736205
                    if state == 8:
                        return -1.22641194
            if state == 2:
                return -18.8475571
    elif num_booster == 86:
        state = 0
        if state == 0:
            state = (1 if x['f8']<61 else 2)
            if state == 1:
                state = (3 if x['f7']<76 or math.isnan(x['f7'])  else 4)
                if state == 3:
                    state = (7 if x['f4']<31 else 8)
                    if state == 7:
                        return 0.0275093894
                    if state == 8:
                        return -0.307228804
                if state == 4:
                    state = (9 if x['f10']<4 else 10)
                    if state == 9:
                        return 26.1036358
                    if state == 10:
                        return 2.37062144
            if state == 2:
                state = (5 if x['f7']<70 else 6)
                if state == 5:
                    state = (11 if x['f4']<24 else 12)
                    if state == 11:
                        return 0.217473537
                    if state == 12:
                        return 2.60214472
                if state == 6:
                    state = (13 if x['f6']<10 else 14)
                    if state == 13:
                        return -0.750664175
                    if state == 14:
                        return 0.210334808
    elif num_booster == 87:
        state = 0
        if state == 0:
            state = (1 if x['f5']<20 else 2)
            if state == 1:
                state = (3 if x['f5']<15 else 4)
                if state == 3:
                    state = (7 if x['f1']<5 else 8)
                    if state == 7:
                        return -0.331960469
                    if state == 8:
                        return 0.190421864
                if state == 4:
                    state = (9 if x['f8']<103 else 10)
                    if state == 9:
                        return 0.193126127
                    if state == 10:
                        return 8.43196011
            if state == 2:
                state = (5 if x['f2']<2 else 6)
                if state == 5:
                    state = (11 if x['f7']<151 else 12)
                    if state == 11:
                        return -0.653164506
                    if state == 12:
                        return -5.49267721
                if state == 6:
                    state = (13 if x['f2']<11 else 14)
                    if state == 13:
                        return 0.257073641
                    if state == 14:
                        return -0.153504983
    elif num_booster == 88:
        state = 0
        if state == 0:
            state = (1 if x['f8']<23 else 2)
            if state == 1:
                state = (3 if x['f7']<20 else 4)
                if state == 3:
                    state = (7 if x['f8']<21 else 8)
                    if state == 7:
                        return -0.179915726
                    if state == 8:
                        return 0.75037545
                if state == 4:
                    state = (9 if x['f4']<13 else 10)
                    if state == 9:
                        return -0.190020055
                    if state == 10:
                        return 0.898548245
            if state == 2:
                state = (5 if x['f8']<25 else 6)
                if state == 5:
                    state = (11 if x['f4']<14 else 12)
                    if state == 11:
                        return -1.13563454
                    if state == 12:
                        return -0.321920335
                if state == 6:
                    state = (13 if x['f4']<15 else 14)
                    if state == 13:
                        return 0.165267557
                    if state == 14:
                        return -0.176709443
    elif num_booster == 89:
        state = 0
        if state == 0:
            state = (1 if x['f6']<39 else 2)
            if state == 1:
                state = (3 if x['f6']<38 else 4)
                if state == 3:
                    state = (7 if x['f8']<91 else 8)
                    if state == 7:
                        return 0.104333118
                    if state == 8:
                        return -0.392524302
                if state == 4:
                    state = (9 if x['f8']<94 else 10)
                    if state == 9:
                        return 0.0743941739
                    if state == 10:
                        return 6.08954
            if state == 2:
                state = (5 if x['f8']<334 else 6)
                if state == 5:
                    state = (11 if x['f7']<262 else 12)
                    if state == 11:
                        return -0.167207807
                    if state == 12:
                        return -6.18607998
                if state == 6:
                    state = (13 if x['f5']<32 else 14)
                    if state == 13:
                        return -3.71101117
                    if state == 14:
                        return 25.7376842
    elif num_booster == 90:
        state = 0
        if state == 0:
            state = (1 if x['f0']<2 else 2)
            if state == 1:
                state = (3 if x['f9']<11 else 4)
                if state == 3:
                    state = (7 if x['f8']<124 else 8)
                    if state == 7:
                        return 0.618478239
                    if state == 8:
                        return 6.25098658
                if state == 4:
                    state = (9 if x['f8']<209 else 10)
                    if state == 9:
                        return 0.0773958489
                    if state == 10:
                        return -3.66257668
            if state == 2:
                state = (5 if x['f8']<209 else 6)
                if state == 5:
                    state = (11 if x['f8']<200 else 12)
                    if state == 11:
                        return -0.0669655874
                    if state == 12:
                        return -2.67809415
                if state == 6:
                    state = (13 if x['f9']<20 else 14)
                    if state == 13:
                        return 2.04757047
                    if state == 14:
                        return -3.38412547
    elif num_booster == 91:
        state = 0
        if state == 0:
            state = (1 if x['f8']<334 else 2)
            if state == 1:
                state = (3 if x['f7']<328 or math.isnan(x['f7'])  else 4)
                if state == 3:
                    state = (7 if x['f7']<262 or math.isnan(x['f7'])  else 8)
                    if state == 7:
                        return -0.00334711187
                    if state == 8:
                        return 1.22095299
                if state == 4:
                    state = (9 if x['f10']<19 else 10)
                    if state == 9:
                        return 2.31061196
                    if state == 10:
                        return 38.9867058
            if state == 2:
                state = (5 if x['f11']<2 else 6)
                if state == 5:
                    state = (11 if x['f3']<4 else 12)
                    if state == 11:
                        return -12.6543837
                    if state == 12:
                        return 17.3559093
                if state == 6:
                    state = (13 if x['f9']<8 else 14)
                    if state == 13:
                        return -17.6971779
                    if state == 14:
                        return -1.12824726
    elif num_booster == 92:
        state = 0
        if state == 0:
            state = (1 if x['f2']<15 else 2)
            if state == 1:
                state = (3 if x['f7']<328 else 4)
                if state == 3:
                    state = (7 if x['f8']<334 else 8)
                    if state == 7:
                        return -0.0995432064
                    if state == 8:
                        return 19.7914505
                if state == 4:
                    state = (9 if x['f5']<6 else 10)
                    if state == 9:
                        return 22.1456013
                    if state == 10:
                        return -7.04145956
            if state == 2:
                state = (5 if x['f7']<328 or math.isnan(x['f7'])  else 6)
                if state == 5:
                    state = (11 if x['f8']<334 else 12)
                    if state == 11:
                        return 0.0661087483
                    if state == 12:
                        return -18.4469738
                if state == 6:
                    state = (13 if x['f10']<19 else 14)
                    if state == 13:
                        return -0.15552184
                    if state == 14:
                        return 17.5641766
    elif num_booster == 93:
        state = 0
        if state == 0:
            state = (1 if x['f10']<7 else 2)
            if state == 1:
                state = (3 if x['f8']<426 else 4)
                if state == 3:
                    state = (7 if x['f8']<159 else 8)
                    if state == 7:
                        return -0.168532982
                    if state == 8:
                        return -6.09237242
                if state == 4:
                    state = (9 if x['f2']<2 else 10)
                    if state == 9:
                        return 160.839401
                    if state == 10:
                        return -9.0612936
            if state == 2:
                state = (5 if x['f8']<426 else 6)
                if state == 5:
                    state = (11 if x['f7']<425 or math.isnan(x['f7'])  else 12)
                    if state == 11:
                        return 0.0212127343
                    if state == 12:
                        return 11.5209217
                if state == 6:
                    state = (13 if x['f5']<21 else 14)
                    if state == 13:
                        return 5.732759
                    if state == 14:
                        return -9.06480026
    elif num_booster == 94:
        state = 0
        if state == 0:
            state = (1 if x['f7']<175 else 2)
            if state == 1:
                state = (3 if x['f8']<209 else 4)
                if state == 3:
                    state = (7 if x['f8']<200 else 8)
                    if state == 7:
                        return -0.0221121814
                    if state == 8:
                        return 14.2265806
                if state == 4:
                    state = (9 if x['f4']<24 else 10)
                    if state == 9:
                        return 0.226720259
                    if state == 10:
                        return -33.147747
            if state == 2:
                state = (5 if x['f9']<11 else 6)
                if state == 5:
                    state = (11 if x['f4']<14 else 12)
                    if state == 11:
                        return 0.579832733
                    if state == 12:
                        return -6.90470648
                if state == 6:
                    state = (13 if x['f2']<7 else 14)
                    if state == 13:
                        return 4.71839666
                    if state == 14:
                        return 0.285415173
    elif num_booster == 95:
        state = 0
        if state == 0:
            state = (1 if x['f9']<8 else 2)
            if state == 1:
                state = (3 if x['f6']<30 else 4)
                if state == 3:
                    state = (7 if x['f7']<97 else 8)
                    if state == 7:
                        return 0.364502996
                    if state == 8:
                        return 4.09529686
                if state == 4:
                    state = (9 if x['f6']<31 else 10)
                    if state == 9:
                        return -7.98609304
                    if state == 10:
                        return 0.180052266
            if state == 2:
                state = (5 if x['f4']<4 else 6)
                if state == 5:
                    state = (11 if x['f8']<293 else 12)
                    if state == 11:
                        return -0.20504342
                    if state == 12:
                        return -17.1776829
                if state == 6:
                    state = (13 if x['f7']<425 or math.isnan(x['f7'])  else 14)
                    if state == 13:
                        return -0.0125557426
                    if state == 14:
                        return 1.75163102
    elif num_booster == 96:
        state = 0
        if state == 0:
            state = (1 if x['f5']<34 else 2)
            if state == 1:
                state = (3 if x['f5']<33 else 4)
                if state == 3:
                    state = (7 if x['f7']<215 else 8)
                    if state == 7:
                        return 0.020619791
                    if state == 8:
                        return -0.977037728
                if state == 4:
                    state = (9 if x['f7']<262 or math.isnan(x['f7'])  else 10)
                    if state == 9:
                        return -0.989651978
                    if state == 10:
                        return 13.337924
            if state == 2:
                state = (5 if x['f7']<425 else 6)
                if state == 5:
                    state = (11 if x['f8']<334 else 12)
                    if state == 11:
                        return 0.125644073
                    if state == 12:
                        return 8.15372849
                if state == 6:
                    state = (13 if x['f4']<6 else 14)
                    if state == 13:
                        return -39.2280998
                    if state == 14:
                        return -2.08906198
    elif num_booster == 97:
        state = 0
        if state == 0:
            state = (1 if x['f3']<3 else 2)
            if state == 1:
                state = (3 if x['f8']<247 else 4)
                if state == 3:
                    state = (7 if x['f8']<219 else 8)
                    if state == 7:
                        return -0.128238007
                    if state == 8:
                        return -6.63773584
                if state == 4:
                    state = (9 if x['f9']<6 else 10)
                    if state == 9:
                        return -32.137001
                    if state == 10:
                        return 4.29247093
            if state == 2:
                state = (5 if x['f8']<268 else 6)
                if state == 5:
                    state = (11 if x['f8']<219 else 12)
                    if state == 11:
                        return 0.0466698185
                    if state == 12:
                        return 1.99477327
                if state == 6:
                    state = (13 if x['f5']<36 else 14)
                    if state == 13:
                        return 1.43804038
                    if state == 14:
                        return -8.1075182
    elif num_booster == 98:
        state = 0
        if state == 0:
            state = (1 if x['f5']<25 else 2)
            if state == 1:
                state = (3 if x['f7']<242 else 4)
                if state == 3:
                    state = (7 if x['f8']<334 else 8)
                    if state == 7:
                        return -0.0444481596
                    if state == 8:
                        return 39.4627762
                if state == 4:
                    state = (9 if x['f5']<7 else 10)
                    if state == 9:
                        return -7.03258133
                    if state == 10:
                        return 0.658963799
            if state == 2:
                state = (5 if x['f7']<425 or math.isnan(x['f7'])  else 6)
                if state == 5:
                    state = (11 if x['f4']<14 else 12)
                    if state == 11:
                        return -0.173431918
                    if state == 12:
                        return 0.243104845
                if state == 6:
                    state = (13 if x['f0']<3 else 14)
                    if state == 13:
                        return -4.35662508
                    if state == 14:
                        return 16.4846554
    elif num_booster == 99:
        state = 0
        if state == 0:
            state = (1 if x['f5']<9 else 2)
            if state == 1:
                state = (3 if x['f8']<179 else 4)
                if state == 3:
                    state = (7 if x['f8']<174 else 8)
                    if state == 7:
                        return 0.0967120901
                    if state == 8:
                        return -11.9966726
                if state == 4:
                    state = (9 if x['f2']<7 else 10)
                    if state == 9:
                        return 16.134304
                    if state == 10:
                        return 0.413386166
            if state == 2:
                state = (5 if x['f5']<10 else 6)
                if state == 5:
                    state = (11 if x['f8']<426 else 12)
                    if state == 11:
                        return -0.357830316
                    if state == 12:
                        return -11.3550558
                if state == 6:
                    state = (13 if x['f2']<8 else 14)
                    if state == 13:
                        return -0.289339393
                    if state == 14:
                        return 0.0602042638

def xgb_predict(x):
    predict = 14.015040887850468
    # initialize prediction with base score
    for i in range(100):
        predict = predict + xgb_tree(x, i)
    return predict