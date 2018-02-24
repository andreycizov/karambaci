import matplotlib as mpl
from mpl_toolkits.mplot3d import Axes3D
import numpy as np
import matplotlib.pyplot as plt

mpl.rcParams['legend.fontsize'] = 10

fig = plt.figure()
ax = fig.gca(projection='3d')
theta = np.linspace(-4 * np.pi, 4 * np.pi, 100)
z = np.linspace(-5, 3, 100)


def plot(arg, c):
    r = arg ** z
    x = r.imag
    y = r.real
    ax.plot(x, z, y, f'{c}-', label=f'{c} - {arg:.2f}')


colors = ['b', 'y', 'r', 'g', 'c', 'm']


def colorsies():
    while True:
        yield from colors


import math

one = 0.1 + 0j
its = 40
deg_start = math.pi * -0.25
deg_end = math.pi * 1.75
deg_diff = deg_end - deg_start

for x, c in zip(range(its), colorsies()):
    deg_curr = deg_start + x * (deg_diff / its)

    mod = math.cos(deg_curr) + math.sin(deg_curr) * 1j

    plot(one * mod, c=c)

ax.legend()

plt.show()
