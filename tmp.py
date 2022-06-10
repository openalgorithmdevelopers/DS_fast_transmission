from matplotlib.animation import FuncAnimation
import matplotlib.pyplot as plt
import pandas as pd

import numpy as np
from scipy.io.wavfile import write

df = pd.read_csv('signal_captured_on_1June22.csv')
y = df['signal']
data = np.array(y)

#    data = np.random.uniform(-1,1,44100) # 44100 random samples between -1 and 1
scaled = np.int16(data/np.max(np.abs(data)) * 32767)
# write('test.wav', 44100, scaled)
write('test.wav', 1000, scaled)