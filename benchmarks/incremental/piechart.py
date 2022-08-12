import matplotlib.pyplot as plt
from matplotlib import gridspec
import math
import matplotlib.patches as mpatches
import numpy as np

class Graph:
    def __init__(self, data):
        self.data = data
        self.num_cols = len(data[0])
        self.num_rows = len(data)
        self.num_grid_cols = 1000

    def plot(self):
        fig = plt.figure(figsize=(6, 4))
        for i in range(self.num_rows):
            row = self.data[i]
            cur_col = 0
            for j in range(self.num_cols):
                col = row[j]
                colspan = self.get_num_cols(i, j)
                print(colspan)
                ax = plt.subplot2grid((self.num_rows, self.num_grid_cols), (i, cur_col), colspan=colspan)
                cur_col += colspan
                ax.pie(col.get_data())
        plt.show()

    def get_num_cols(self, row, col):
        total = 0
        for step in self.data[row]:
            total += sum(step.get_data())
        val = int((sum(self.data[row][col].get_data()) / total) * self.num_grid_cols)
        if val == 0:
            return val + 1
        else:
            return val

class Step:
    def __init__(self, fast_path, slow_path, write):
        self.fast_path = fast_path
        self.slow_path = slow_path
        self.write = write

    def get_data(self):
        return [self.fast_path, self.slow_path, self.write]

    def get_ratio(self):
        return math.sqrt(sum(self.get_data()))

def main():
    # data = [
    #     [Step(190.9079, 0, 2.381094), Step(17.8558, 6.106002, 6.650906), Step(15.79836, 2.799532, 6.771469), Step(16.50858, 2.214784, 6.747705), Step(15.70112, 1.991375, 6.759192), Step(15.06146, 0.7536615, 6.796239), Step(14.98719, 0.8343657, 6.780956)],
    #     [Step(191.0442, 0, 2.409159), Step(0, 9.969823, 0.06659347), Step(0, 3.194658, 0.001719711), Step(0, 2.255742, 0.00131141), Step(0, 2.273456, 0.001078326), Step(0, 1.590703, 0.0008960344), Step(0, 0.1210075, 0.0004500517)]
    # ]

    data = [Step(190.9079, 0, 2.381094), Step(17.8558, 6.106002, 6.650906), Step(15.79836, 2.799532, 6.771469), Step(16.50858, 2.214784, 6.747705), Step(15.70112, 1.991375, 6.759192), Step(15.06146, 0.7536615, 6.796239), Step(14.98719, 0.8343657, 6.780956)]
    # data = [Step(191.0442, 0, 2.409159), Step(0, 9.969823, 0.06659347), Step(0, 3.194658, 0.001719711), Step(0, 2.255742, 0.00131141), Step(0, 2.273456, 0.001078326), Step(0, 1.590703, 0.0008960344), Step(0, 0.1210075, 0.0004500517)]
    # data = [Step(0, 9.969823, 0.06659347), Step(0, 3.194658, 0.001719711), Step(0, 2.255742, 0.00131141), Step(0, 2.273456, 0.001078326), Step(0, 1.590703, 0.0008960344), Step(0, 0.1210075, 0.0004500517)]

    # graph = Graph(data)
    # graph.plot()
    # for i in range(num_rows):
    #     row = data[i]
    #     for j in range(num_cols):
    #         col = row[j]
    #         ax = plt.subplot2grid((num_rows, num_cols), (i, j))
    #         ax.pie(col.data())
    #         # ax.axis('equal')
    #
    # plt.show()

    fig, axes = plt.subplots(1, len(data), figsize=(6, 4), sharex=True, gridspec_kw={'width_ratios': [d.get_ratio() for d in data]})
    for i, step in enumerate(data):
        axes[i].pie(step.get_data())

    plt.suptitle('Time breakdown')
    labels = ['0', '1', '2', '3', '4', '5', '6']
    plt.xticks(np.arange(len(labels)), labels)
    # plt.legend(handles=[
    #     # mpatches.Patch(label='Fast Path'),
    #     # mpatches.Patch(label='Slow Path'),
    #     # mpatches.Patch(label='Write')
    # ])

    plt.show()

    # p1 = Step(190.9079, 0, 2.381094)
    # p2 = Step(17.8558, 6.106002, 6.650906)

    # gs1 = gridspec.GridSpec(1, 2, width_ratios=[p1.ratio(), p2.ratio()])

    # i1 = Step(191.0442, 0, 2.409159)
    # i2 = Step(0, 9.969823, 0.09659347)

    # gs2 = gridspec.GridSpec(1, 2, width_ratios=[i1.ratio(), i2.ratio()])

    # fig = plt.figure(figsize=(6, 4))
    # fig, axes = plt.subplots(1, 2, figsize=(6, 4), gridspec_kw={'width_ratios': [p1.ratio(), p2.ratio()]})
    # ax1 = plt.subplot2grid((2, 100), (0, 0), colspan=86, rowspan=1)
    # ax2 = plt.subplot2grid((2, 100), (0, 86), colspan=14, rowspan=1)
    # ax3 = plt.subplot2grid((2, 100), (1, 0), colspan=95, rowspan=1)
    # ax4 = plt.subplot2grid((2, 100), (1, 95), colspan=5, rowspan=1)

    # axes[0].pie([p1.fast_path, p1.slow_path, p1.write])
    # axes[1].pie([p2.fast_path, p2.slow_path, p2.write])

    # axes.pie([i1.fast_path, i1.slow_path, i1.write])
    # ax4.pie([i2.fast_path, i2.slow_path, i2.write])

    # plt.tight_layout()


# fig, axes = plt.subplots(rows, cols)
    # fig.suptitle('Out of Order Time Breakdown')
    #
    # for row in range(rows):
    #     for col in range(cols):
    #         axes[row, col].pie([10, 20, 70])

    # plt.show()

if __name__ == '__main__':
    main()